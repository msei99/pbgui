from __future__ import annotations

import asyncio
import getpass
import json
import os
import psutil
import re
import shlex
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
import yaml

from api.vps import get_monitor, get_monitor_state_snapshot, get_metric_history_snapshot
from logging_helpers import human_log as _log
from master.async_monitor import INSTANCE_COLLECT_SCRIPT, METRICS_STREAM_STALE_SECONDS, MONITOR_CACHE_VERSION
from MonitorConfig import MonitorConfig
from PBCoinData import CoinData
from PBRemote import PBRemote
from pb7_release import build_local_pb7_release_info, get_current_pb7_status, load_more_pb7_commits
from pbgui_release import build_local_pbgui_release_info, load_more_pbgui_commits
from pbgui_purefunc import get_git_branch_remote, get_git_branch_remotes, get_git_remote_url, list_git_remotes, list_remote_git_branch_commits, list_remote_git_branches, load_ini, load_ini_section, pb7dir as configured_pb7dir, save_ini, save_ini_section
from vps_manager_core import PBGDIR, VPS, VPSManager, _install_dir_from_remote_pbgui_dir, strip_ansi

SERVICE = "VPSManagerApi"

PB7_UPSTREAM_REMOTE_NAME = "origin"
PB7_UPSTREAM_REMOTE_URL = "https://github.com/enarjord/passivbot.git"
SWAP_OPTIONS = ["0", "1G", "1.5G", "2G", "2.5G", "3G", "4G", "5G", "6G", "8G"]
INIT_METHODS = ["root", "password", "private_key"]
SESSION_SECRET_TTL_SECONDS = 15 * 60
# Guardrail: every field listed here is sensitive bootstrap/auth material and
# must never be written to host JSON or included in normal config/detail payloads.
SECRET_FIELDS = (
    "user_pw",
    "initial_root_pw",
    "root_pw",
    "user_sudo",
    "user_sudo_pw",
    "private_key_user",
    "private_key_file",
)

ROLLING_PEAK_WINDOW_SECONDS = 60.0
VPS_LOGGING_SERVICES = ("PBRun", "PBRemote", "PBCoinData", "sync", "vps_cleanup", "tradfi_sync")
VPS_LOGGING_DEFAULT_MB = 1
VPS_LOGGING_CLEANUP_MB = 64 / 1024
VPS_LOGGING_DEPLOY_HISTORY_FILE = Path(PBGDIR) / "data" / "vpsmanager" / "vps_logging_deploy_history.json"
VPS_LOGGING_DEPLOY_HISTORY_LIMIT = 20
VPS_DEPLOY_SECTION = "vps_deploy"
COMMAND_VPS_DEPLOY_LOGGING = "vps-deploy-logging"
COMMAND_VPS_UPDATE = "vps-update"
COMMAND_VPS_UPDATE_PBGUI = "vps-update-pbgui"
COMMAND_VPS_UPDATE_PB7 = "vps-update-pb7"
COMMAND_VPS_UPDATE_PB = "vps-update-pb"
COMMAND_VPS_CLEANUP = "vps-cleanup"
VPS_DEPLOY_DEFAULT_ACTION = COMMAND_VPS_DEPLOY_LOGGING
VPS_DEPLOY_DEFAULT_MODE = "parallel"
VPS_DEPLOY_MODES = ("parallel", "sequential")
VPS_DEPLOY_ACTION_TEXT = {
    COMMAND_VPS_DEPLOY_LOGGING: "Deploy Settings",
    COMMAND_VPS_UPDATE: "Update Linux",
    COMMAND_VPS_UPDATE_PBGUI: "Update PBGui",
    COMMAND_VPS_UPDATE_PB7: "Update PB7",
    COMMAND_VPS_UPDATE_PB: "Update PBGui and PB7",
    COMMAND_VPS_CLEANUP: "Cleanup VPS",
}
VPS_DEPLOY_ACTIONS = tuple(VPS_DEPLOY_ACTION_TEXT.keys())
VPS_DEPLOY_HISTORY_FILE = VPS_LOGGING_DEPLOY_HISTORY_FILE
VPS_DEPLOY_HISTORY_LIMIT = VPS_LOGGING_DEPLOY_HISTORY_LIMIT
DEPLOY_PROGRESS_LOG_TAIL_BYTES = 64 * 1024
DEPLOY_PROGRESS_CACHE_LIMIT = 256
DEPLOY_RUN_APPEAR_TIMEOUT_SECONDS = 30
_PLAYBOOK_TASK_CACHE: dict[str, tuple[str, ...]] = {}


class UnknownHostKeyError(ValueError):
    def __init__(self, *, hostname: str, ssh_host: str, ip: str) -> None:
        self.hostname = str(hostname or "")
        self.ssh_host = str(ssh_host or "")
        self.ip = str(ip or "")
        target = self.ssh_host or self.ip or self.hostname
        super().__init__(f"Server '{target}' not found in known_hosts")


def _now_ts() -> int:
    return round(datetime.now().timestamp())


def _today_start_ts() -> int:
    return round(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())


def _valid_ipv4(value: str) -> bool:
    if not value:
        return False
    return bool(
        re.match(
            r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
            str(value).strip(),
        )
    )


def _short_commit(value: str | None) -> str:
    return str(value or "")[:7]


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _status_running(status: str | None) -> bool:
    normalized = str(status or "").strip().lower()
    return normalized not in {"", "none", "successful", "failed", "error", "timeout", "canceled", "cancelled", "interrupted"}


def _playbook_task_names(command: str | None) -> tuple[str, ...]:
    task_name = str(command or "").strip()
    if not task_name:
        return ()
    cached = _PLAYBOOK_TASK_CACHE.get(task_name)
    if cached is not None:
        return cached

    playbook_path = Path(PBGDIR) / f"{task_name}.yml"
    tasks: list[str] = []
    try:
        payload = yaml.safe_load(playbook_path.read_text(encoding="utf-8"))
        plays = payload if isinstance(payload, list) else [payload]
        for play in plays:
            if not isinstance(play, dict):
                continue
            if bool(play.get("gather_facts", False)):
                tasks.append("Gathering Facts")
            for section in ("pre_tasks", "tasks", "post_tasks", "handlers"):
                for item in play.get(section) or []:
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("name") or "").strip()
                    if name:
                        tasks.append(name)
    except Exception as exc:
        _log(SERVICE, f"failed to load playbook tasks for {task_name}: {exc}", level="WARNING")
        _PLAYBOOK_TASK_CACHE[task_name] = ()
        return ()

    deduped: list[str] = []
    seen: set[str] = set()
    for item in tasks:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    result = tuple(deduped)
    _PLAYBOOK_TASK_CACHE[task_name] = result
    return result


def _local_no_new_privileges() -> bool:
    try:
        status_path = Path("/proc/self/status")
        if not status_path.exists():
            return False
        for line in status_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("NoNewPrivs:"):
                return line.split(":", 1)[1].strip() == "1"
    except Exception:
        return False
    return False


def _metric_level(value: float, warning: float, error: float, *, inverse: bool = False) -> str:
    if inverse:
        if value <= error:
            return "error"
        if value <= warning:
            return "warning"
        return "ok"
    if value >= error:
        return "error"
    if value >= warning:
        return "warning"
    return "ok"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_float_str(value: Any, default: float) -> float:
    try:
        return float(str(value or "").strip())
    except Exception:
        return default


def _python_major_minor(executable: str | None) -> str:
    candidate = str(executable or "").strip()
    if not candidate:
        return "N/A"
    path = Path(candidate)
    if not path.exists():
        return "N/A"
    try:
        res = subprocess.run(
            [candidate, "-c", "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
        )
    except Exception:
        return "N/A"
    if res.returncode != 0:
        return "N/A"
    return str(res.stdout or "").strip() or "N/A"


def _configured_pb7dir() -> str:
    return str(configured_pb7dir() or "").strip()


def _parse_pbgui_release_number(version: str | None) -> tuple[int, ...]:
    text = str(version or "").strip()
    match = re.search(r"v?(\d+(?:\.\d+)*)", text)
    if not match:
        return ()
    parts: list[int] = []
    for item in match.group(1).split('.'):
        try:
            parts.append(int(item))
        except Exception:
            return ()
    return tuple(parts)


def _version_gte(version: str | None, minimum: str) -> bool:
    current = _parse_pbgui_release_number(version)
    target = _parse_pbgui_release_number(minimum)
    if not current or not target:
        return False
    max_len = max(len(current), len(target))
    current = current + (0,) * (max_len - len(current))
    target = target + (0,) * (max_len - len(target))
    return current >= target


def _normalize_vps_deploy_command(value: Any) -> str:
    command = str(value or VPS_DEPLOY_DEFAULT_ACTION).strip()
    if command not in VPS_DEPLOY_ACTIONS:
        return VPS_DEPLOY_DEFAULT_ACTION
    return command


def _normalize_vps_deploy_mode(value: Any) -> str:
    mode = str(value or VPS_DEPLOY_DEFAULT_MODE).strip().lower()
    if mode not in VPS_DEPLOY_MODES:
        return VPS_DEPLOY_DEFAULT_MODE
    return mode


def _vps_deploy_command_text(command: Any) -> str:
    normalized = _normalize_vps_deploy_command(command)
    return VPS_DEPLOY_ACTION_TEXT.get(normalized, normalized)


def _vps_deploy_requires_user_password(command: Any) -> bool:
    normalized = _normalize_vps_deploy_command(command)
    return normalized in {COMMAND_VPS_UPDATE, COMMAND_VPS_CLEANUP}


def _get_file_sync_worker():
    try:
        from api.api_keys import _file_sync_worker
        return _file_sync_worker
    except Exception:
        return None


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    try:
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return []
        return [item for item in payload if isinstance(item, dict)]
    except Exception:
        _log(SERVICE, f"Failed to load deploy history from {path}", level="warning")
        return []


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp_path.write_text(json.dumps(payload, indent=4), encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


def _read_text_tail(path: Path, *, max_bytes: int = DEPLOY_PROGRESS_LOG_TAIL_BYTES) -> str:
    try:
        if not path.exists():
            return ""
        limit = max(1, int(max_bytes))
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            if size <= 0:
                return ""
            start = max(size - limit, 0)
            handle.seek(start, os.SEEK_SET)
            data = handle.read(limit)
        if start > 0:
            newline = data.find(b"\n")
            if newline != -1 and newline + 1 < len(data):
                data = data[newline + 1:]
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _read_vps_update_log_tail(vps: VPS) -> str:
    candidates = [
        vps._task_log_path(vps.command, COMMAND_VPS_UPDATE),
        vps._task_log_alias_path(vps.command, COMMAND_VPS_UPDATE),
    ]
    for path in candidates:
        log_text = _read_text_tail(path)
        if log_text:
            return strip_ansi(log_text)
    return strip_ansi(str(getattr(vps, "update_log", "") or ""))


def _extract_playbook_run_started_at(log_text: str) -> str:
    text = str(log_text or "")
    if not text:
        return ""
    match = re.search(r"^=== PLAYBOOK RUN START\s+(.+?)\s+===$", text, flags=re.MULTILINE)
    return str(match.group(1) or "").strip() if match else ""


def _read_playbook_run_started_at(path: Path) -> str:
    try:
        if not path.exists():
            return ""
        with path.open("rb") as handle:
            header = handle.read(512)
        return _extract_playbook_run_started_at(header.decode("utf-8", errors="ignore"))
    except Exception:
        return ""


def _parse_ansible_task_progress(hostname: str, log_text: str, *, command: str | None = None) -> dict[str, Any]:
    host = str(hostname or "").strip()
    text = str(log_text or "")
    playbook_tasks = _playbook_task_names(command)
    started_at = _extract_playbook_run_started_at(text)
    if not host or not text:
        return {
            "started_at": started_at,
            "step": "",
            "step_kind": "",
            "result": "",
            "recap": None,
            "current_index": 0,
            "total_steps": len(playbook_tasks),
            "current_label": "",
        }

    normalized = text.replace("\r", "\n")
    task_matches = list(re.finditer(r"(TASK|RUNNING HANDLER) \[(.*?)\]", normalized))
    step_kind = ""
    step = ""
    tail = normalized
    if task_matches:
        last_task = task_matches[-1]
        step_kind = str(last_task.group(1) or "").strip().lower()
        step = str(last_task.group(2) or "").strip()
        tail = normalized[last_task.start():]

    host_pattern = re.escape(host)
    result = ""
    result_patterns = (
        ("failed", rf"(?:fatal|failed): \[{host_pattern}\]"),
        ("unreachable", rf"unreachable: \[{host_pattern}\]"),
        ("changed", rf"changed: \[{host_pattern}\]"),
        ("ok", rf"ok: \[{host_pattern}\]"),
        ("skipping", rf"skipping: \[{host_pattern}\]"),
    )
    for label, pattern in result_patterns:
        if re.search(pattern, tail):
            result = label
            break

    recap = None
    recap_match = re.search(
        rf"^{host_pattern}\s*:\s*ok=(\d+)\s+changed=(\d+)\s+unreachable=(\d+)\s+failed=(\d+)\s+skipped=(\d+)\s+rescued=(\d+)\s+ignored=(\d+)",
        normalized,
        flags=re.MULTILINE,
    )
    if recap_match:
        recap = {
            "ok": int(recap_match.group(1)),
            "changed": int(recap_match.group(2)),
            "unreachable": int(recap_match.group(3)),
            "failed": int(recap_match.group(4)),
            "skipped": int(recap_match.group(5)),
            "rescued": int(recap_match.group(6)),
            "ignored": int(recap_match.group(7)),
        }

    total_steps = len(playbook_tasks)
    current_index = 0
    if recap:
        current_index = total_steps
    elif step:
        try:
            current_index = playbook_tasks.index(step)
        except ValueError:
            current_index = 0

    return {
        "started_at": started_at,
        "step": step,
        "step_kind": step_kind,
        "result": result,
        "recap": recap,
        "current_index": current_index,
        "total_steps": total_steps,
        "current_label": step,
    }


def _build_vps_logging_phase(task_progress: dict[str, Any], task_status: str) -> dict[str, Any]:
    step = str((task_progress or {}).get("step") or "").strip().lower()
    status = str(task_status or "").strip().lower()

    phases = [
        {"key": "config", "label": "Write config"},
        {"key": "cap", "label": "Force single-file"},
        {"key": "restart_core", "label": "Restart PBRun/PBRemote"},
        {"key": "restart_coindata", "label": "Restart PBCoinData"},
    ]

    current_key = ""
    if "write vps logging section values" in step:
        current_key = "config"
    elif "force single-file capped logging on vps" in step:
        current_key = "cap"
    elif "restart pbrun and pbremote after logging deploy" in step:
        current_key = "restart_core"
    elif "restart pbcoindata after logging deploy" in step:
        current_key = "restart_coindata"

    if status == "successful":
        current_index = len(phases)
    elif current_key:
        current_index = next((idx + 1 for idx, item in enumerate(phases) if item["key"] == current_key), 1)
    elif status in {"failed", "error", "timeout", "cancelled", "canceled"}:
        current_index = 1
    elif status:
        current_index = 1
    else:
        current_index = 0

    return {
        "current": current_index,
        "total": len(phases),
        "label": "Done" if status == "successful" else next((item["label"] for item in phases if item["key"] == current_key), "Waiting"),
        "steps": [
            {
                "key": item["key"],
                "label": item["label"],
                "state": "done" if idx < current_index else "current" if idx + 1 == current_index and status != "successful" else "pending",
            }
            for idx, item in enumerate(phases)
        ],
    }


class VPSManagerService:
    def __init__(self):
        self.vpsmanager = VPSManager()
        self.pbremote: PBRemote | None = None
        self.coindata: CoinData | None = None
        self.monitor_config = MonitorConfig()
        self._first_refresh_done = False
        self._pbgui_release: dict[str, Any] = {}
        self._pbgui_release_ts = 0
        self._pb7_release: dict[str, Any] = {}
        self._pb7_release_ts = 0
        self._local_package_status: dict[str, Any] = {"upgrades": "N/A", "reboot": False}
        self._vps_package_status_cache: dict[str, dict[str, Any]] = {}
        # Quick detail is pushed every second. Any status that requires a slower
        # validation step must reuse the last full-detail result instead of
        # falling back to a weaker default on the next quick push.
        self._master_coindata_ok_cache: bool = False
        self._master_monitor_payload_cache: dict[str, Any] | None = None
        self._master_monitor_cache: dict[str, Any] = {"_version": MONITOR_CACHE_VERSION}
        self._master_bot_cpu_history: dict[str, dict[str, Any]] = {}
        self._master_server_metric_history: dict[str, list[tuple[float, float]]] = {
            "memory": [],
            "disk": [],
            "swap": [],
        }
        self._vps_coindata_status_cache: dict[str, bool] = {}
        self._vps_ssh_ok_cache: dict[str, bool] = {}
        self._session_secrets: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
        self._setup_api_sync_done: dict[str, str] = {}
        self._deploy_threads: dict[str, threading.Thread] = {}
        self._deploy_sessions: dict[str, dict[str, Any]] = {}
        self._deploy_sessions_lock = threading.Lock()
        self._deploy_history_lock = threading.Lock()
        self._deploy_progress_cache_lock = threading.Lock()
        self._deploy_progress_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._bucket_cleanup_indicator_lock = threading.Lock()
        self._bucket_cleanup_indicator_running = False
        self._bucket_cleanup_indicator: dict[str, Any] = {
            "state": "unknown",
            "count": 0,
            "hostnames": [],
            "checked_at": 0,
            "error": "",
        }
        self._recover_interrupted_vps_runs()

    def _invalidate_bucket_cleanup_indicator(self) -> None:
        with self._bucket_cleanup_indicator_lock:
            self._bucket_cleanup_indicator_running = False
            self._bucket_cleanup_indicator = {
                "state": "unknown",
                "count": 0,
                "hostnames": [],
                "checked_at": 0,
                "error": "",
            }

    def get_bucket_cleanup_indicator(self, *, force: bool = False) -> dict[str, Any]:
        with self._bucket_cleanup_indicator_lock:
            cached = dict(self._bucket_cleanup_indicator or {})
            if self._bucket_cleanup_indicator_running:
                cached["running"] = True
                return cached
            if not force and str(cached.get("state") or "unknown") != "unknown":
                cached["running"] = False
                return cached
            self._bucket_cleanup_indicator_running = True

        result = {
            "state": "error",
            "count": 0,
            "hostnames": [],
            "checked_at": _now_ts(),
            "error": "Bucket cleanup indicator failed.",
        }
        try:
            pbremote = self._ensure_pbremote()
            bucket = str(getattr(pbremote, "bucket", "") or "").strip()
            if not bucket:
                result = {
                    "state": "error",
                    "count": 0,
                    "hostnames": [],
                    "checked_at": _now_ts(),
                    "error": "PBRemote bucket is not configured.",
                }
            elif not getattr(pbremote, "rclone_installed", False):
                result = {
                    "state": "error",
                    "count": 0,
                    "hostnames": [],
                    "checked_at": _now_ts(),
                    "error": "rclone is not installed locally.",
                }
            else:
                ok, entries = pbremote.list_bucket_entries()
                if not ok:
                    result = {
                        "state": "error",
                        "count": 0,
                        "hostnames": [],
                        "checked_at": _now_ts(),
                        "error": str(entries or "Failed to list bucket entries."),
                    }
                else:
                    hostnames: set[str] = set()
                    for raw_entry in entries or []:
                        entry = str(raw_entry or "").strip().rstrip("/")
                        if entry.startswith("cmd_"):
                            hostname = entry[4:].split("/", 1)[0].strip()
                            if hostname:
                                hostnames.add(hostname)
                        elif entry.startswith("run_v7_"):
                            hostname = entry[7:].split("/", 1)[0].strip()
                            if hostname:
                                hostnames.add(hostname)
                    ordered = sorted(hostnames)
                    result = {
                        "state": "dirty" if ordered else "clean",
                        "count": len(ordered),
                        "hostnames": ordered,
                        "checked_at": _now_ts(),
                        "error": "",
                    }
        except Exception as exc:
            result = {
                "state": "error",
                "count": 0,
                "hostnames": [],
                "checked_at": _now_ts(),
                "error": str(exc) or "Bucket cleanup indicator failed.",
            }
        finally:
            with self._bucket_cleanup_indicator_lock:
                self._bucket_cleanup_indicator = result
                self._bucket_cleanup_indicator_running = False
                result = dict(self._bucket_cleanup_indicator)

        result["running"] = False
        return result

    def _is_vps_playbook_process_running(self, vps: VPS) -> bool:
        if not self._should_treat_vps_process_as_active(vps):
            return False
        host_tmp_dir = str((Path(vps.path) / "tmp").resolve()) if vps.path else ""
        inventory_path = str((Path(host_tmp_dir) / "inventory" / "hosts").resolve()) if host_tmp_dir else ""
        playbook_path = str((Path(PBGDIR) / f"{str(vps.command or '').strip()}.yml").resolve())
        if not host_tmp_dir:
            return False
        try:
            for proc in psutil.process_iter(["cmdline"]):
                try:
                    cmdline = [str(part or "") for part in (proc.info.get("cmdline") or [])]
                except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
                    continue
                if not cmdline:
                    continue
                normalized = {part for part in cmdline if part}
                if not any("ansible-playbook" in part or "ansible-runner" in part for part in normalized):
                    continue
                if any(part.startswith(f"{host_tmp_dir}/") or part == host_tmp_dir for part in normalized):
                    return True
                if inventory_path in normalized and playbook_path in normalized:
                    return True
            return False
        except Exception as exc:
            _log(SERVICE, f"failed to inspect VPS playbook processes for {vps.hostname}: {exc}", level="WARNING")
            return False

    def _should_treat_vps_process_as_active(self, vps: VPS) -> bool:
        if _status_running(getattr(vps, "init_status", None)) or _status_running(getattr(vps, "setup_status", None)):
            return True
        if not _status_running(getattr(vps, "update_status", None)):
            return False
        log_text = _read_vps_update_log_tail(vps)
        if not log_text:
            return True
        parsed_progress = _parse_ansible_task_progress(str(vps.hostname or ""), log_text, command=vps.command)
        return self._status_from_task_progress(parsed_progress) not in {"successful", "failed", "unreachable"}

    def _is_master_playbook_process_running(self) -> bool:
        if not self._should_treat_master_process_as_active():
            return False
        master_tmp_dir = str((Path(PBGDIR) / "data" / "vpsmanager" / "tmp").resolve())
        playbook_path = str((Path(PBGDIR) / f"{str(getattr(self.vpsmanager, 'command', '') or '').strip()}.yml").resolve())
        try:
            for proc in psutil.process_iter(["cmdline"]):
                try:
                    cmdline = [str(part or "") for part in (proc.info.get("cmdline") or [])]
                except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
                    continue
                if not cmdline:
                    continue
                normalized = {part for part in cmdline if part}
                if not any("ansible-playbook" in part or "ansible-runner" in part for part in normalized):
                    continue
                if any(part.startswith(f"{master_tmp_dir}/") or part == master_tmp_dir for part in normalized):
                    return True
                if playbook_path in normalized:
                    return True
            return False
        except Exception as exc:
            _log(SERVICE, f"failed to inspect master playbook processes: {exc}", level="WARNING")
            return False

    def _should_treat_master_process_as_active(self) -> bool:
        if not _status_running(getattr(self.vpsmanager, "update_status", None)):
            return False
        log_text = strip_ansi(str(getattr(self.vpsmanager, "update_log", "") or ""))
        command = str(getattr(self.vpsmanager, "command", "") or "").strip()
        hostname = str(getattr(self.vpsmanager, "hostname", "local") or "local").strip() or "local"
        if not log_text:
            return True
        parsed_progress = _parse_ansible_task_progress(hostname, log_text, command=command)
        return self._status_from_task_progress(parsed_progress) not in {"successful", "failed", "unreachable"}

    def _recover_interrupted_vps_runs(self) -> None:
        recovered_hosts: list[str] = []
        interrupted_hosts: list[str] = []
        for vps in self.vpsmanager.vpss:
            if not str(vps.hostname or "").strip() or not _status_running(vps.update_status):
                continue
            log_text = _read_vps_update_log_tail(vps)
            parsed_progress = _parse_ansible_task_progress(str(vps.hostname or ""), log_text, command=vps.command)
            parsed_status = self._status_from_task_progress(parsed_progress)
            if parsed_status in {"successful", "failed", "unreachable"}:
                vps.update_status = "failed" if parsed_status == "unreachable" else parsed_status
                if not str(vps.last_update or "").strip():
                    vps.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                vps.save()
                recovered_hosts.append(str(vps.hostname))
                continue
            if self._is_vps_playbook_process_running(vps):
                continue
            vps.update_status = "interrupted"
            vps.save()
            interrupted_hosts.append(str(vps.hostname))
        if recovered_hosts:
            _log(SERVICE, f"recovered completed VPS deploy state for: {', '.join(sorted(recovered_hosts))}", level="WARNING")
        if interrupted_hosts:
            _log(SERVICE, f"marked interrupted VPS deploys after API restart/crash: {', '.join(sorted(interrupted_hosts))}", level="WARNING")

    def has_active_vps_deploys(self) -> bool:
        for vps in self.vpsmanager.vpss:
            if _status_running(vps.init_status) or _status_running(vps.setup_status) or _status_running(vps.update_status):
                return True
            if self._is_vps_playbook_process_running(vps):
                return True
        if _status_running(getattr(self.vpsmanager, "update_status", None)) or self._is_master_playbook_process_running():
            return True
        return False

    def active_vps_deploy_summary(self) -> dict[str, Any]:
        items: list[dict[str, str]] = []
        for vps in self.vpsmanager.vpss:
            hostname = str(vps.hostname or "").strip()
            if not hostname:
                continue
            for phase, status, command_text in (
                ("init", str(vps.init_status or "").strip(), "Initialize"),
                ("setup", str(vps.setup_status or "").strip(), "Setup VPS"),
                ("update", str(vps.update_status or "").strip(), str(vps.command_text or _vps_deploy_command_text(vps.command))),
            ):
                if not _status_running(status):
                    continue
                items.append({
                    "hostname": hostname,
                    "phase": phase,
                    "status": status,
                    "command_text": command_text,
                })
            if self._is_vps_playbook_process_running(vps) and not any(
                item["hostname"] == hostname for item in items
            ):
                items.append({
                    "hostname": hostname,
                    "phase": "process",
                    "status": "running",
                    "command_text": str(vps.command_text or _vps_deploy_command_text(vps.command) or "Ansible Task"),
                })
        master_status = str(getattr(self.vpsmanager, "update_status", "") or "").strip()
        if _status_running(master_status) or self._is_master_playbook_process_running():
            items.append({
                "hostname": "local",
                "phase": "master",
                "status": master_status or "running",
                "command_text": str(getattr(self.vpsmanager, "command_text", "") or "Master Task"),
            })
        return {
            "active": bool(items),
            "items": items,
            "summary": ", ".join(f"{item['hostname']} {item['command_text']} ({item['status']})" for item in items[:5]),
        }

    def _ensure_pbremote(self, *, force_reinit: bool = False) -> PBRemote:
        if self.pbremote is None or force_reinit:
            self.pbremote = PBRemote()
        return self.pbremote

    def _ensure_coindata(self) -> CoinData:
        if self.coindata is None:
            self.coindata = CoinData()
        return self.coindata

    def _get_pbgui_release(self) -> dict[str, Any]:
        return self._pbgui_release or {}

    def _refresh_pbgui_release(self) -> None:
        self._pbgui_release = build_local_pbgui_release_info()
        self._pbgui_release_ts = _now_ts()

    def _get_pb7_release(self) -> dict[str, Any]:
        return self._pb7_release or {}

    def _refresh_pb7_release(self, repo_dir: str | None) -> None:
        self._pb7_release = build_local_pb7_release_info(repo_dir)
        self._pb7_release_ts = _now_ts()

    def _get_local_package_status(self) -> dict[str, Any]:
        return self._local_package_status or {"upgrades": "N/A", "reboot": False}

    def _refresh_local_package_status(self) -> None:
        upgrades: str | int = "N/A"
        reboot_required = Path("/var/run/reboot-required").exists()
        try:
            result = subprocess.run(
                ["apt-get", "dist-upgrade", "-s"],
                text=True,
                timeout=15,
                env={**os.environ, "LANG": "C"},
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            if result.returncode == 0:
                match = re.search(r"(\d+) upgraded", result.stdout or "")
                if match:
                    upgrades = match.group(1)
        except FileNotFoundError:
            upgrades = "N/A"
        except subprocess.TimeoutExpired:
            _log(SERVICE, "local package probe timed out", level="WARNING")
        except Exception as exc:
            _log(SERVICE, f"local package probe failed: {exc}", level="WARNING")
        self._local_package_status = {
            "upgrades": upgrades,
            "reboot": reboot_required,
        }

    def clear_session_secrets(self, token: str) -> None:
        token = str(token or "").strip()
        if token:
            self._session_secrets.pop(token, None)

    def prune_session_secrets(self, valid_tokens: set[str] | None = None) -> None:
        now = _now_ts()
        next_store: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
        for token, host_map in self._session_secrets.items():
            if valid_tokens is not None and token not in valid_tokens:
                continue
            next_host_map: dict[str, dict[str, dict[str, Any]]] = {}
            for hostname, field_map in host_map.items():
                next_field_map: dict[str, dict[str, Any]] = {}
                for field_name, payload in field_map.items():
                    if payload.get("value") and int(payload.get("expires_at") or 0) > now:
                        next_field_map[field_name] = payload
                if next_field_map:
                    next_host_map[hostname] = next_field_map
            if next_host_map:
                next_store[token] = next_host_map
        self._session_secrets = next_store

    def _get_secret_bucket(self, token: str, hostname: str, *, create: bool = False) -> dict[str, dict[str, Any]] | None:
        token = str(token or "").strip()
        hostname = str(hostname or "").strip()
        if not token or not hostname:
            return None
        self.prune_session_secrets()
        host_map = self._session_secrets.get(token)
        if host_map is None:
            if not create:
                return None
            host_map = {}
            self._session_secrets[token] = host_map
        bucket = host_map.get(hostname)
        if bucket is None and create:
            bucket = {}
            host_map[hostname] = bucket
        return bucket

    def _session_secret_payload(self, value: str) -> dict[str, Any]:
        now = _now_ts()
        return {
            "value": value,
            "stored_at": now,
            "expires_at": now + SESSION_SECRET_TTL_SECONDS,
        }

    def _store_session_secrets(self, token: str, hostname: str, form: dict[str, Any]) -> None:
        bucket = self._get_secret_bucket(token, hostname, create=True)
        if bucket is None:
            return
        # Only the token-scoped in-memory store may retain these fields.
        # Do not mirror them into persisted VPS config or generic API payloads.
        for field_name in SECRET_FIELDS:
            if field_name not in form:
                continue
            value = str(form.get(field_name) or "")
            if value:
                bucket[field_name] = self._session_secret_payload(value)
            else:
                bucket.pop(field_name, None)
        if not bucket:
            host_map = self._session_secrets.get(str(token or "").strip()) or {}
            host_map.pop(str(hostname or "").strip(), None)

    def _secret_entry(self, token: str, hostname: str, field_name: str) -> dict[str, Any] | None:
        bucket = self._get_secret_bucket(token, hostname, create=False)
        if not bucket:
            return None
        entry = bucket.get(field_name)
        if not entry:
            return None
        if int(entry.get("expires_at") or 0) <= _now_ts():
            bucket.pop(field_name, None)
            if not bucket:
                host_map = self._session_secrets.get(str(token or "").strip()) or {}
                host_map.pop(str(hostname or "").strip(), None)
            return None
        return entry

    def _session_secret_value(self, token: str, hostname: str, field_name: str) -> str:
        entry = self._secret_entry(token, hostname, field_name)
        return str(entry.get("value") or "") if entry else ""

    def _session_secret_meta(self, token: str, hostname: str) -> dict[str, Any]:
        hostname = str(hostname or "")
        now = _now_ts()
        out: dict[str, Any] = {}
        for field_name in SECRET_FIELDS:
            entry = self._secret_entry(token, hostname, field_name)
            expires_at = int(entry.get("expires_at") or 0) if entry else 0
            out[field_name] = {
                "stored": entry is not None,
                "expires_at": expires_at,
                "remaining_seconds": max(expires_at - now, 0),
            }
        return out

    def reveal_session_secret(self, token: str, hostname: str, field_name: str) -> dict[str, Any]:
        if field_name not in SECRET_FIELDS:
            raise ValueError("Unsupported secret field.")
        meta = self._session_secret_meta(token, hostname).get(field_name) or {}
        return {
            "hostname": str(hostname or ""),
            "field": field_name,
            "value": self._session_secret_value(token, hostname, field_name),
            "stored": bool(meta.get("stored")),
            "expires_at": int(meta.get("expires_at") or 0),
            "remaining_seconds": int(meta.get("remaining_seconds") or 0),
        }

    def _apply_session_secrets_to_vps(self, token: str, vps: VPS) -> None:
        hostname = str(vps.hostname or "")
        for field_name in SECRET_FIELDS:
            value = self._session_secret_value(token, hostname, field_name)
            setattr(vps, field_name, value or None)

    def _require_user_password(self, token: str, hostname: str) -> str:
        value = self._session_secret_value(token, hostname, "user_pw")
        if not value:
            raise ValueError("VPS user password expired or missing. Please enter it again.")
        return value

    def _sync_vps_inventory(self) -> None:
        pattern = str(Path(f"{PBGDIR}/data/vpsmanager/hosts/*/*.json"))
        host_files = sorted(Path(path) for path in __import__("glob").glob(pattern, recursive=False))
        current = {item.hostname: item for item in self.vpsmanager.vpss if item.hostname}
        next_items: list[VPS] = []
        existing_hosts: set[str] = set()
        for host_file in host_files:
            loaded = VPS()
            loaded.load(str(host_file))
            if not loaded.hostname:
                continue
            existing_hosts.add(loaded.hostname)
            current_item = current.get(loaded.hostname)
            if current_item is None:
                next_items.append(loaded)
                continue
            if not (
                _status_running(current_item.init_status)
                or _status_running(current_item.setup_status)
                or _status_running(current_item.update_status)
            ):
                current_item.load(str(host_file))
            next_items.append(current_item)
        for item in self.vpsmanager.vpss:
            if item.hostname and item.hostname not in existing_hosts:
                if _status_running(item.init_status) or _status_running(item.setup_status) or _status_running(item.update_status):
                    next_items.append(item)
        self.vpsmanager.vpss = sorted(next_items, key=lambda entry: entry.hostname or "")
        if not _status_running(self.vpsmanager.update_status):
            self.vpsmanager.load_master()

    def refresh(self, *, force: bool = False) -> None:
        self._sync_vps_inventory()
        pbremote = self._ensure_pbremote(force_reinit=force and self.pbremote is not None and bool(self.pbremote.error))
        if getattr(pbremote, "local_run", None) is None:
            self._first_refresh_done = True
            return

        try:
            self._refresh_pbgui_release()
            self._refresh_pb7_release(_configured_pb7dir())
        except Exception as exc:
            _log(SERVICE, f"refresh local versions failed: {exc}", level="WARNING")

        # Package update and reboot state should refresh on every master detail
        # fetch so localhost maintenance actions immediately reflect the new
        # pending-update count instead of waiting for the next full refresh.
        try:
            self._refresh_local_package_status()
        except Exception as exc:
            _log(SERVICE, f"refresh package status failed: {exc}", level="WARNING")

        stale = (_now_ts() - int(self._pbgui_release_ts or 0)) > 3600
        full_refresh = force or stale or not self._first_refresh_done
        if full_refresh:
            try:
                self._refresh_pbgui_release()
            except Exception as exc:
                _log(SERVICE, f"refresh git origin failed: {exc}", level="WARNING")
            try:
                self._refresh_pb7_release(_configured_pb7dir())
            except Exception as exc:
                _log(SERVICE, f"refresh local commit data failed: {exc}", level="WARNING")

        self._first_refresh_done = True

    def _get_monitor_state(self) -> dict[str, Any]:
        try:
            live_state = get_monitor_state_snapshot()
            connections = ((live_state.get("connections") or {}).get("connections") or {})
            has_live_data = bool(
                connections
                or (live_state.get("system") or {})
                or (live_state.get("instances") or {})
                or (live_state.get("v7_instances") or {})
                or (live_state.get("host_meta") or {})
                or (live_state.get("streams") or {})
            )
            if has_live_data:
                return live_state
            return live_state
        except Exception as exc:
            _log(SERVICE, f"monitor snapshot failed: {exc}", level="WARNING")
            return {
                "connections": {"connections": {}},
                "system": {},
                "instances": {},
                "v7_instances": {},
                "host_meta": {},
                "streams": {},
            }

    def _get_host_telemetry(self, monitor_state: dict[str, Any], hostname: str) -> dict[str, Any]:
        connections = ((monitor_state.get("connections") or {}).get("connections") or {})
        return {
            "hostname": hostname,
            "connection": connections.get(hostname) or {},
            "system": (monitor_state.get("system") or {}).get(hostname) or {},
            "instances": (monitor_state.get("instances") or {}).get(hostname) or [],
            "v7_instances": (monitor_state.get("v7_instances") or {}).get(hostname) or [],
            "meta": (monitor_state.get("host_meta") or {}).get(hostname) or {},
            "stream": (monitor_state.get("streams") or {}).get(hostname) or {},
        }

    def _host_online(self, host_state: dict[str, Any] | None) -> bool:
        if not host_state:
            return False
        status = str((host_state.get("connection") or {}).get("status") or "")
        return status == "connected"

    def _host_telemetry_last_update(self, host_state: dict[str, Any] | None) -> float:
        if not host_state:
            return 0.0
        stream = (host_state or {}).get("stream") or {}
        system = (host_state or {}).get("system") or {}
        values: list[float] = []
        for raw in (stream.get("last_update"), system.get("timestamp")):
            value = _safe_float(raw, 0.0)
            if value > 0:
                values.append(value)
        return max(values) if values else 0.0

    def _host_telemetry_age(self, host_state: dict[str, Any] | None) -> float | None:
        last_update = self._host_telemetry_last_update(host_state)
        if last_update <= 0:
            return None
        return max(time.time() - last_update, 0.0)

    def _host_telemetry_fresh(self, host_state: dict[str, Any] | None) -> bool:
        if not self._host_online(host_state):
            return False
        stream = (host_state or {}).get("stream") or {}
        if stream.get("stale"):
            return False
        age = self._host_telemetry_age(host_state)
        return age is not None and age <= METRICS_STREAM_STALE_SECONDS

    def _host_meta(self, host_state: dict[str, Any] | None) -> dict[str, Any]:
        return (host_state or {}).get("meta") or {}

    def _refresh_host_meta_now(self, hostname: str, *, include_package_status: bool = True) -> None:
        monitor = get_monitor()
        if monitor is None:
            return
        host = str(hostname or "").strip()
        if not host:
            return
        try:
            loop = getattr(monitor, "loop", None)
            if loop is None or loop.is_closed():
                _log(SERVICE, f"immediate host-meta refresh skipped for {host}: monitor loop unavailable", level="WARNING")
                return
            asyncio.run_coroutine_threadsafe(
                monitor.collect_host_meta_now(host, include_package_status=include_package_status),
                loop,
            ).result(timeout=30)
        except Exception as exc:
            _log(SERVICE, f"immediate host-meta refresh failed for {host}: {exc}", level="WARNING")

    def _bucket_cleanup_preview_rows(self) -> list[dict[str, Any]]:
        pbremote = self._ensure_pbremote()
        bucket = str(getattr(pbremote, "bucket", "") or "").strip()
        local_hostname = str(getattr(pbremote, "name", "") or "").strip()
        if not bucket:
            raise ValueError("PBRemote bucket is not configured.")
        if not getattr(pbremote, "rclone_installed", False):
            raise ValueError("rclone is not installed locally.")
        ok, result = pbremote.list_bucket_entries()
        if not ok:
            raise ValueError(str(result or "Failed to list bucket entries."))

        monitor_state = self._get_monitor_state()
        by_hostname = {str(vps.hostname or "").strip(): vps for vps in self.vpsmanager.vpss if str(vps.hostname or "").strip()}
        if local_hostname:
            by_hostname.setdefault(local_hostname, pbremote)
        bucket_hosts: dict[str, dict[str, Any]] = {}
        for raw_entry in result or []:
            entry = str(raw_entry or "").strip().rstrip("/")
            if entry.startswith("cmd_"):
                hostname = entry[4:]
                if hostname:
                    bucket_hosts.setdefault(hostname, {"cmd": False, "run_v7": False})["cmd"] = True
            elif entry.startswith("run_v7_"):
                hostname = entry[7:]
                if hostname:
                    bucket_hosts.setdefault(hostname, {"cmd": False, "run_v7": False})["run_v7"] = True

        rows: list[dict[str, Any]] = []
        for hostname in sorted(bucket_hosts.keys()):
            host_state = self._get_host_telemetry(monitor_state, hostname)
            meta = self._host_meta(host_state)
            if not meta and hostname == local_hostname:
                meta = {"role": "master"}
            role = str(meta.get("role") or "unknown").strip().lower() or "unknown"
            version = str(meta.get("pbgv") or "N/A")
            known = hostname in by_hostname
            eligible = known and role == "slave" and _version_gte(version, "v1.78")
            if eligible:
                category = "eligible"
                reason = "Eligible: slave with PBGui >= v1.78"
            elif known:
                category = "known"
                reasons: list[str] = []
                if role != "slave":
                    reasons.append(f"role={role or 'unknown'}")
                if hostname != local_hostname and not _version_gte(version, "v1.78"):
                    reasons.append(f"PBGui {version or 'unknown'} < v1.78")
                reason = ", ".join(reasons) if reasons else "Known VPS but not eligible"
            else:
                category = "orphaned"
                reason = "Bucket entry has no matching VPS Manager record"
            rows.append({
                "hostname": hostname,
                "cmd": bool(bucket_hosts[hostname].get("cmd")),
                "run_v7": bool(bucket_hosts[hostname].get("run_v7")),
                "known": known,
                "role": role,
                "pbgui_version": version,
                "eligible": eligible,
                "category": category,
                "reason": reason,
            })
        return rows

    def preview_bucket_cleanup(self) -> dict[str, Any]:
        pbremote = self._ensure_pbremote()
        rows = self._bucket_cleanup_preview_rows()
        return {
            "bucket": str(getattr(pbremote, "bucket", "") or ""),
            "rows": rows,
            "summary": {
                "total": len(rows),
                "eligible": sum(1 for item in rows if item.get("category") == "eligible"),
                "known": sum(1 for item in rows if item.get("category") == "known"),
                "orphaned": sum(1 for item in rows if item.get("category") == "orphaned"),
            },
        }

    def cleanup_bucket(self, hostnames: list[Any]) -> dict[str, Any]:
        pbremote = self._ensure_pbremote()
        selected: list[str] = []
        seen: set[str] = set()
        for raw_host in hostnames or []:
            hostname = str(raw_host or "").strip()
            if not hostname or hostname in seen:
                continue
            seen.add(hostname)
            selected.append(hostname)
        if not selected:
            raise ValueError("Select at least one bucket entry to delete.")

        preview_rows = {str(item.get("hostname") or ""): item for item in self._bucket_cleanup_preview_rows()}
        results: list[dict[str, Any]] = []
        for hostname in selected:
            preview = dict(preview_rows.get(hostname) or {"hostname": hostname, "category": "orphaned", "eligible": False, "reason": "Selected manually"})
            dry_run_info = pbremote.cleanup_bucket_host_entries_dry_run(hostname)
            dry_run_ok = bool(dry_run_info.get("ok"))
            dry_run_matches = [str(item or '').strip().rstrip('/') for item in (dry_run_info.get("matches") or []) if str(item or '').strip()]
            allowed_prefixes = (f'cmd_{hostname}', f'run_v7_{hostname}')
            if not dry_run_ok:
                results.append({
                    **preview,
                    "ok": False,
                    "deleted": [],
                    "error": str(dry_run_info.get("error") or "Bucket dry-run failed."),
                    "dry_run_matches": dry_run_matches,
                })
                continue
            unexpected = [item for item in dry_run_matches if item not in allowed_prefixes and not item.startswith((f'{allowed_prefixes[0]}/', f'{allowed_prefixes[1]}/'))]
            if unexpected:
                results.append({
                    **preview,
                    "ok": False,
                    "deleted": [],
                    "error": f"Bucket dry-run matched unexpected paths: {', '.join(unexpected[:5])}",
                    "dry_run_matches": dry_run_matches,
                })
                continue
            cleanup_result = pbremote.cleanup_bucket_host_entries(hostname)
            results.append({
                **preview,
                "ok": bool(cleanup_result.get("ok")),
                "deleted": list(cleanup_result.get("deleted") or []),
                "error": str(cleanup_result.get("error") or ""),
                "dry_run_matches": dry_run_matches,
                "operations": list(cleanup_result.get("operations") or []),
            })
        payload = {
            "bucket": str(getattr(pbremote, "bucket", "") or ""),
            "results": results,
            "summary": {
                "requested": len(selected),
                "deleted": sum(1 for item in results if item.get("ok")),
                "failed": sum(1 for item in results if not item.get("ok")),
            },
        }
        self._invalidate_bucket_cleanup_indicator()
        return payload

    def dry_run_bucket_cleanup(self, hostnames: list[Any]) -> dict[str, Any]:
        pbremote = self._ensure_pbremote()
        selected: list[str] = []
        seen: set[str] = set()
        for raw_host in hostnames or []:
            hostname = str(raw_host or "").strip()
            if not hostname or hostname in seen:
                continue
            seen.add(hostname)
            selected.append(hostname)
        if not selected:
            raise ValueError("Select at least one bucket entry first.")

        preview_rows = {str(item.get("hostname") or ""): item for item in self._bucket_cleanup_preview_rows()}
        results: list[dict[str, Any]] = []
        for hostname in selected:
            preview = dict(preview_rows.get(hostname) or {"hostname": hostname, "category": "orphaned", "eligible": False, "reason": "Selected manually"})
            dry_run = pbremote.cleanup_bucket_host_entries_dry_run(hostname)
            matches = [str(item or '').strip().rstrip('/') for item in (dry_run.get("matches") or []) if str(item or '').strip()]
            results.append({
                **preview,
                "ok": bool(dry_run.get("ok")),
                "matches": matches,
                "match_count": len(matches),
                "error": str(dry_run.get("error") or ""),
            })
        return {
            "bucket": str(getattr(pbremote, "bucket", "") or ""),
            "results": results,
            "summary": {
                "requested": len(selected),
                "ok": sum(1 for item in results if item.get("ok")),
                "failed": sum(1 for item in results if not item.get("ok")),
                "matches": sum(int(item.get("match_count") or 0) for item in results),
            },
        }

    def build_state(self) -> dict[str, Any]:
        self.refresh(force=False)
        pbremote = self._ensure_pbremote()
        monitor_state = self._get_monitor_state()
        overview_rows = self._build_overview_rows(pbremote, monitor_state)
        coindata = self._ensure_coindata()
        cmc_api_key = coindata.api_key or ""
        vps_logging = self.get_vps_logging_config()
        deploy_settings = self.get_vps_deploy_settings()
        deploy_history = self.get_vps_deploy_history()
        deploy_progress_rows = self._build_deploy_progress_rows(overview_rows, deploy_history)
        return {
            "config": {
                "master_name": getattr(pbremote, "name", "local"),
                "local_user": getpass.getuser(),
                "swap_options": SWAP_OPTIONS,
                "init_methods": INIT_METHODS,
                "bucket": getattr(pbremote, "bucket", "") or "",
                "coinmarketcap_api_key": cmc_api_key,
                "vps_logging": vps_logging,
                "vps_deploy": deploy_settings,
            },
            "errors": self._build_errors(pbremote),
            "overview": {
                "rows": overview_rows,
            },
            "deploys": {
                "history": deploy_history,
                "vps_logging_history": deploy_history,
                "progress_rows": deploy_progress_rows,
                "vps_logging_progress_rows": deploy_progress_rows,
            },
        }

    def _progress_payload(self, status: str, current_index: int, total_steps: int) -> dict[str, Any]:
        normalized_status = str(status or "").strip().lower()
        safe_current = max(0, int(current_index or 0))
        safe_total = max(0, int(total_steps or 0))
        if normalized_status == "successful":
            done = safe_total or safe_current
            total = safe_total or safe_current
            percent = 100 if total else 0
            return {"done": done, "total": total, "percent": percent}
        done = safe_current
        total = safe_total
        percent = max(0, min(100, (done / total) * 100)) if total else 0
        return {"done": done, "total": total, "percent": percent}

    def _prune_deploy_progress_cache(self, active_keys: set[tuple[str, str, str]] | None = None) -> None:
        active = set(active_keys or set())
        with self._deploy_progress_cache_lock:
            if active:
                stale_keys = [key for key in list(self._deploy_progress_cache.keys()) if key not in active]
                for key in stale_keys:
                    self._deploy_progress_cache.pop(key, None)
            if len(self._deploy_progress_cache) <= DEPLOY_PROGRESS_CACHE_LIMIT:
                return
            overflow = len(self._deploy_progress_cache) - DEPLOY_PROGRESS_CACHE_LIMIT
            for key in list(self._deploy_progress_cache.keys())[:overflow]:
                self._deploy_progress_cache.pop(key, None)

    def _status_from_task_progress(self, task_progress: dict[str, Any]) -> str:
        progress = task_progress or {}
        recap = progress.get("recap")
        if isinstance(recap, dict):
            if int(recap.get("failed") or 0) > 0 or int(recap.get("unreachable") or 0) > 0:
                return "failed"
            return "successful"
        result = str(progress.get("result") or "").strip().lower()
        if result in {"failed", "unreachable"}:
            return result
        if str(progress.get("step") or "").strip() or str(progress.get("started_at") or "").strip():
            return "running"
        return ""

    def _build_deploy_progress_rows(self, overview_rows: list[dict[str, Any]], deploy_history: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not deploy_history:
            return []
        if not isinstance(deploy_history[0], dict):
            _log(SERVICE, "deploy progress skipped: latest history entry is invalid", level="WARNING")
            return []
        latest_entry = deploy_history[0]
        latest_command = str(latest_entry.get("command") or "vps-deploy-logging")
        latest_command_text = str(latest_entry.get("command_text") or _vps_deploy_command_text(latest_command))
        latest_started_at = str(latest_entry.get("started_at") or "")
        latest_host_logs = latest_entry.get("host_logs") if isinstance(latest_entry.get("host_logs"), dict) else {}
        deploy_hosts = [str(host).strip() for host in latest_entry.get("hostnames") or [] if str(host).strip()]
        if not deploy_hosts:
            return []

        by_hostname = {
            str((row or {}).get("hostname") or "").strip(): row
            for row in overview_rows or []
            if str((row or {}).get("hostname") or "").strip()
        }
        progress_rows: list[dict[str, Any]] = []
        active_cache_keys: set[tuple[str, str, str]] = set()
        for hostname in deploy_hosts:
            base_row = dict(by_hostname.get(hostname) or {"hostname": hostname, "name": hostname})
            host_log_meta = latest_host_logs.get(hostname) if isinstance(latest_host_logs.get(hostname), dict) else {}
            expected_started_at = str(host_log_meta.get("started_at") or latest_started_at or "").strip()
            expected_run_id = str(host_log_meta.get("run_id") or "").strip()
            file_alias = str(host_log_meta.get("file_alias") or "").strip()
            filename = str(host_log_meta.get("filename") or "").strip()

            parsed_progress: dict[str, Any] | None = None
            parsed_status = ""
            if filename:
                log_path = Path(PBGDIR) / "data" / "vpsmanager" / "hosts" / hostname / filename
                cache_key = (hostname, filename, latest_command)
                active_cache_keys.add(cache_key)
                with self._deploy_progress_cache_lock:
                    cache_entry = dict(self._deploy_progress_cache.get(cache_key) or {}) or None
                stat_signature: tuple[int, int] | None = None
                try:
                    stat = log_path.stat()
                    stat_signature = (int(stat.st_mtime_ns), int(stat.st_size))
                except Exception:
                    stat_signature = None
                if cache_entry and cache_entry.get("stat_signature") == stat_signature:
                    parsed_progress = dict(cache_entry.get("parsed_progress") or {}) or None
                    parsed_status = str(cache_entry.get("parsed_status") or "")
                elif stat_signature is not None:
                    log_text = _read_text_tail(log_path)
                    if log_text:
                        parsed_progress = _parse_ansible_task_progress(hostname, log_text, command=latest_command)
                        parsed_started_at = str(parsed_progress.get("started_at") or "").strip()
                        if not parsed_started_at:
                            parsed_started_at = _read_playbook_run_started_at(log_path)
                            if parsed_started_at:
                                parsed_progress["started_at"] = parsed_started_at
                        if parsed_started_at and (not expected_started_at or parsed_started_at == expected_started_at):
                            parsed_status = self._status_from_task_progress(parsed_progress)
                            with self._deploy_progress_cache_lock:
                                self._deploy_progress_cache[cache_key] = {
                                    "stat_signature": stat_signature,
                                    "parsed_progress": dict(parsed_progress),
                                    "parsed_status": parsed_status,
                                }
                        else:
                            parsed_progress = None
                            parsed_status = ""
                    else:
                        with self._deploy_progress_cache_lock:
                            self._deploy_progress_cache.pop(cache_key, None)
                else:
                    with self._deploy_progress_cache_lock:
                        self._deploy_progress_cache.pop(cache_key, None)

            live_command = str(base_row.get("task_command") or "").strip()
            live_run_id = str(base_row.get("task_run_id") or "").strip()
            live_status = str(base_row.get("task_status") or "").strip().lower()
            live_matches_latest_run = bool(live_command == latest_command and expected_run_id and live_run_id == expected_run_id)
            live_is_terminal = live_status in {"successful", "failed", "error", "timeout", "canceled", "cancelled", "unreachable"}

            if parsed_progress is not None:
                current_index = int(parsed_progress.get("current_index") or 0)
                total_steps = int(parsed_progress.get("total_steps") or 0)
                row = {
                    **base_row,
                    "task_command": latest_command,
                    "task_command_text": latest_command_text,
                    "task_status": parsed_status,
                    "task_started": str(parsed_progress.get("started_at") or expected_started_at or latest_started_at),
                    "task_step": str(parsed_progress.get("step") or ""),
                    "task_step_kind": str(parsed_progress.get("step_kind") or ""),
                    "task_result": str(parsed_progress.get("result") or ""),
                    "task_recap": parsed_progress.get("recap"),
                    "task_current_index": current_index,
                    "task_total_steps": total_steps,
                    "task_current_label": str(parsed_progress.get("current_label") or parsed_progress.get("step") or ""),
                    "task_progress": self._progress_payload(parsed_status, current_index, total_steps),
                    "task_log_file": file_alias,
                    "task_log_filename": filename,
                }
                progress_rows.append(row)
                continue

            if live_matches_latest_run:
                safe_live_status = "starting" if live_is_terminal else str(base_row.get("task_status") or "starting")
                current_index = int(base_row.get("task_current_index") or 0)
                total_steps = int(base_row.get("task_total_steps") or 0)
                row = {
                    **base_row,
                    "task_command": latest_command,
                    "task_command_text": latest_command_text,
                    "task_status": safe_live_status,
                    "task_started": str(base_row.get("task_started") or expected_started_at or latest_started_at),
                    "task_step": str(base_row.get("task_step") or ""),
                    "task_step_kind": str(base_row.get("task_step_kind") or ""),
                    "task_result": str(base_row.get("task_result") or ""),
                    "task_recap": base_row.get("task_recap"),
                    "task_current_index": current_index,
                    "task_total_steps": total_steps,
                    "task_current_label": str(base_row.get("task_current_label") or base_row.get("task_step") or ""),
                    "task_progress": self._progress_payload(safe_live_status, current_index, total_steps),
                    "task_log_file": file_alias,
                    "task_log_filename": filename,
                }
                progress_rows.append(row)
                continue

            progress_rows.append(
                {
                    **base_row,
                    "task_command": latest_command,
                    "task_command_text": latest_command_text,
                    "task_status": "",
                    "task_started": expected_started_at or latest_started_at,
                    "task_step": "",
                    "task_step_kind": "",
                    "task_result": "",
                    "task_recap": None,
                    "task_current_index": 0,
                    "task_total_steps": 0,
                    "task_current_label": "",
                    "task_progress": {"done": 0, "total": 0, "percent": 0},
                    "task_log_file": file_alias,
                    "task_log_filename": filename,
                }
            )
        self._prune_deploy_progress_cache(active_cache_keys)
        return progress_rows

    def build_master_detail(self) -> dict[str, Any]:
        self.refresh(force=False)
        pbremote = self._ensure_pbremote()
        coindata = self._ensure_coindata()
        coindata_ok = False
        try:
            coindata_ok = coindata.fetch_api_status()
        except Exception:
            coindata_ok = False
        self._master_coindata_ok_cache = bool(coindata_ok)
        master_monitor = self._build_local_master_monitor_payload(pbremote, refresh=True)
        return {
            "kind": "master",
            "status": self._build_master_status(pbremote, coindata_ok),
            "branches": {
                "pbgui": self._build_master_pbgui_branch_state(pbremote),
                "pb7": self._build_master_pb7_branch_state(pbremote),
            },
            "monitor": master_monitor,
            "progress": self._build_master_progress(include_log=True),
        }

    def build_master_detail_quick(self) -> dict[str, Any]:
        pbremote = self._ensure_pbremote()
        return {
            "kind": "master",
            # Quick detail must not overwrite validated full-detail status with
            # a cheap fallback such as a hardcoded False.
            "status": self._build_master_status(pbremote, self._master_coindata_ok_cache),
            "branches": {
                "pbgui": self._build_master_pbgui_branch_state(pbremote),
                "pb7": self._build_master_pb7_branch_state(pbremote),
            },
            "monitor": self._build_local_master_monitor_payload(pbremote, refresh=False),
            "progress": self._build_master_progress(include_log=True),
        }

    def build_vps_detail(self, token: str, hostname: str, *, quick: bool = False) -> dict[str, Any]:
        if not quick:
            self.refresh(force=False)
        pbremote = self._ensure_pbremote()
        vps = self._require_vps(hostname)
        if not quick and str(getattr(vps, "update_status", "") or "").strip().lower() in {"successful", "failed", "error", "timeout", "canceled", "cancelled", "unreachable"}:
            self._refresh_host_meta_now(hostname)
        self._apply_session_secrets_to_vps(token, vps)
        monitor_state = self._get_monitor_state()
        host_state = self._get_host_telemetry(monitor_state, hostname)
        # Quick detail may be less fresh, but it must not regress fields that
        # were already validated by the full-detail path.
        coindata_ok = bool(self._vps_coindata_status_cache.get(hostname, False)) if quick else False
        if not quick:
            try:
                coindata = self._ensure_coindata()
                if vps.coinmarketcap_api_key:
                    old_key = coindata.api_key
                    coindata.api_key = vps.coinmarketcap_api_key
                    coindata_ok = coindata.fetch_api_status()
                    coindata.api_key = old_key
            except Exception:
                coindata_ok = False
            self._vps_coindata_status_cache[hostname] = bool(coindata_ok)

        logfiles: list[str] = []
        monitor_payload = self._build_monitor_payload(host_state, hostname=hostname)
        logfiles.extend(monitor_payload.get("logfiles", []))
        available_logs = ((self._host_meta(host_state).get("available_logs") or []) if host_state else [])
        if isinstance(available_logs, list):
            logfiles.extend(available_logs)
        # add old bot log files
        bot_logs = (monitor_state.get("bot_logs") or {}).get(hostname, {})
        for log_list in bot_logs.values():
            logfiles.extend(log_list)
        return {
            "kind": "vps",
            "hostname": hostname,
            "status": self._build_vps_status(vps, host_state, pbremote, coindata_ok, quick=quick),
            "config": self._build_vps_config(token, vps),
            "branches": {
                "pbgui": self._build_vps_pbgui_branch_state(pbremote, host_state),
                "pb7": self._build_vps_pb7_branch_state(pbremote, host_state, hostname),
            },
            "monitor": monitor_payload,
            "progress": self._build_vps_progress(vps, include_logs=not quick),
            "logfiles": sorted(dict.fromkeys(logfiles)),
            "log_preview": {
                "filename": vps.logfilename or (logfiles[0] if logfiles else ""),
                "size_kb": int(vps.logsize or 50),
                "content": "" if quick else (vps.logfile or ""),
            },
        }

    def get_cpu_history(self, hostname: str, *, bot_name: str = "") -> dict[str, Any]:
        return self.get_metric_history(hostname, bot_name=bot_name, metric="cpu")

    def get_metric_history(self, hostname: str, *, bot_name: str = "", metric: str = "cpu") -> dict[str, Any]:
        hostname = str(hostname or "").strip()
        bot_name = str(bot_name or "").strip()
        if not hostname:
            raise ValueError("Hostname is required.")
        if hostname != self._ensure_pbremote().name:
            self._require_vps(hostname)
        return get_metric_history_snapshot(hostname, bot_name=bot_name, metric=metric)

    def _build_errors(self, pbremote: PBRemote) -> list[str]:
        out: list[str] = []
        if pbremote.error:
            out.append(str(pbremote.error))
        return out

    def _build_overview_rows(self, pbremote: PBRemote,
                             monitor_state: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = [self._build_master_overview_row(pbremote)]
        managed_hostnames: set[str] = set()
        for vps in sorted(self.vpsmanager.vpss, key=lambda item: item.hostname or ""):
            hostname = str(vps.hostname or "")
            if not hostname:
                continue
            managed_hostnames.add(hostname)
            host_state = self._get_host_telemetry(monitor_state, hostname)
            rows.append(self._build_vps_overview_row(pbremote, hostname, host_state))
        return rows

    def _build_master_overview_row(self, pbremote: PBRemote) -> dict[str, Any]:
        pbgui_release = self._get_pbgui_release()
        pb7_release = self._get_pb7_release()
        local_package_status = self._get_local_package_status()
        local_pbgui_python = f"{sys.version_info.major}.{sys.version_info.minor}"
        local_pb7_python = _python_major_minor(load_ini("main", "pb7venv"))
        master_branch = str(pbgui_release.get("current_branch") or "unknown")
        master_commit = str(pbgui_release.get("current_commit") or "")
        master_pb7_branch = str(pb7_release.get("current_branch") or "unknown")
        master_pb7_commit = str(pb7_release.get("current_commit") or "")
        boot_ts = int(getattr(pbremote, "boot", 0) or 0)
        return {
            "name": f"{pbremote.name} (local)",
            "hostname": pbremote.name,
            "nav": "master",
            "online": pbremote.is_running(),
            "role": "master",
            "role_icon": "🧠",
            "start": datetime.fromtimestamp(boot_ts).strftime("%Y-%m-%d %H:%M:%S"),
            "start_ts": boot_ts,
            "reboot_required": bool(local_package_status.get("reboot", False)),
            "updates": local_package_status.get("upgrades", "N/A"),
            "running_bots": "-",
            "pbgui": f"{str(pbgui_release.get('version') or 'N/A')}{'' if local_pbgui_python in (None, '', 'N/A') else ' /' + str(local_pbgui_python)}",
            "pbgui_branch": f"{master_branch} ({_short_commit(master_commit)})",
            "pbgui_github": self._build_master_pbgui_github_status(pbremote, master_branch, master_commit),
            "pb7": f"{str(pb7_release.get('version') or 'N/A')}{'' if local_pb7_python in (None, '', 'N/A') else ' /' + str(local_pb7_python)}",
            "pb7_branch": f"{master_pb7_branch} ({_short_commit(master_pb7_commit)})",
            "pb7_github": self._build_master_pb7_github_status(pbremote, master_pb7_branch, master_pb7_commit),
        }

    def _build_vps_overview_row(self, pbremote: PBRemote,
                                hostname: str,
                                host_state: dict[str, Any]) -> dict[str, Any]:
        vps = self.vpsmanager.find_vps_by_hostname(hostname)
        task_progress = _parse_ansible_task_progress(
            hostname,
            _read_vps_update_log_tail(vps) if vps else "",
            command=str(getattr(vps, "command", "") or "") if vps else "",
        )
        task_phase = _build_vps_logging_phase(task_progress, str(getattr(vps, "update_status", "") or "") if vps else "")
        ssh_online = self._host_online(host_state)
        telemetry_fresh = self._host_telemetry_fresh(host_state)
        telemetry_age = self._host_telemetry_age(host_state)
        online = ssh_online and telemetry_fresh
        meta = self._host_meta(host_state)
        role = str(meta.get("role") or "slave")
        if role == "master":
            role_icon = "🧠"
        else:
            role_icon = "💻"
        boot = _safe_int(meta.get("boot"))
        running_v7_names = {
            str((monitor or {}).get("u") or "").strip()
            for monitor in (host_state or {}).get("instances") or []
            if str((monitor or {}).get("u") or "").strip()
        }
        running_v7_names.update(
            str((instance or {}).get("name") or "").strip()
            for instance in (host_state or {}).get("v7_instances") or []
            if _truthy((instance or {}).get("running")) and str((instance or {}).get("name") or "").strip()
        )
        row = {
            "name": hostname,
            "hostname": hostname,
            "nav": "vps",
            "online": online,
            "ssh_online": ssh_online,
            "telemetry_fresh": telemetry_fresh,
            "telemetry_stale": ssh_online and not telemetry_fresh,
            "telemetry_age": round(telemetry_age, 1) if telemetry_age is not None else None,
            "role": role,
            "role_icon": role_icon,
            "start": datetime.fromtimestamp(boot).strftime("%Y-%m-%d %H:%M:%S") if boot else "",
            "start_ts": boot,
            "reboot_required": bool(meta.get("reboot", False)),
            "updates": meta.get("upgrades", "N/A"),
            "running_bots": len(running_v7_names),
            "pbgui": f"{meta.get('pbgv', 'N/A')}{'' if meta.get('pbgpy', 'N/A') in (None, '', 'N/A') else ' /' + str(meta.get('pbgpy'))}",
            "pbgui_branch": f"{meta.get('pbgb', 'unknown')} ({_short_commit(meta.get('pbgc'))})",
            "pbgui_github": self._build_remote_pbgui_github_status(pbremote, host_state),
            "pb7": f"{meta.get('pb7v', 'N/A')}{'' if meta.get('pb7py', 'N/A') in (None, '', 'N/A') else ' /' + str(meta.get('pb7py'))}",
            "pb7_branch": f"{meta.get('pb7b', 'unknown')} ({_short_commit(meta.get('pb7c'))})",
            "pb7_github": self._build_remote_pb7_github_status(pbremote, host_state),
            "rtd": min(self._build_remote_rtd(host_state), 9999),
            "task_command": str(getattr(vps, "command", "") or "") if vps else "",
            "task_command_text": str(getattr(vps, "command_text", "") or "") if vps else "",
            "task_run_id": str(getattr(vps, "command_run_id", "") or "") if vps else "",
            "task_status": str(getattr(vps, "update_status", "") or "") if vps else "",
            "task_started": str(getattr(vps, "last_update", "") or "") if vps else "",
            "task_step": str(task_progress.get("step") or ""),
            "task_step_kind": str(task_progress.get("step_kind") or ""),
            "task_result": str(task_progress.get("result") or ""),
            "task_recap": task_progress.get("recap"),
            "task_current_index": int(task_progress.get("current_index") or 0),
            "task_total_steps": int(task_progress.get("total_steps") or 0),
            "task_current_label": str(task_progress.get("current_label") or ""),
            "task_phase": task_phase,
        }
        if vps:
            live_package_status = self._get_live_vps_package_status(vps, host_state)
            if live_package_status:
                if live_package_status.get("upgrades") not in (None, ""):
                    row["updates"] = live_package_status.get("upgrades")
                row["reboot_required"] = bool(live_package_status.get("reboot", False))
        return row

    def _build_master_pbgui_github_status(self, pbremote: PBRemote, current_branch: str, current_commit: str) -> str:
        release_info = self._get_pbgui_release()
        branches = release_info.get("branches") or {}
        if current_branch != "unknown" and current_branch in branches and branches[current_branch]:
            origin_commit = branches[current_branch][0]["full"]
            if current_commit == origin_commit:
                return "✅"
            return f"❌ {str(release_info.get('version') or 'N/A')} ({_short_commit(origin_commit)})"
        if current_branch == "main":
            if str(release_info.get("version") or "N/A") == str(release_info.get("origin_version") or "N/A") and current_commit == str(release_info.get("origin_commit") or ""):
                return "✅"
            return f"❌ {str(release_info.get('origin_version') or 'N/A')} ({_short_commit(str(release_info.get('origin_commit') or ''))})"
        return f"⚠️ {str(release_info.get('version') or 'N/A')}"

    def _build_master_pb7_github_status(self, pbremote: PBRemote, current_branch: str, current_commit: str) -> str:
        release_info = self._get_pb7_release()
        branches = release_info.get("branches") or {}
        if current_branch in branches and branches[current_branch]:
            origin_commit = branches[current_branch][0]["full"]
            if current_commit == origin_commit:
                return "✅"
            return f"❌ {str(release_info.get('version') or 'N/A')} ({_short_commit(origin_commit)})"
        if current_branch == "master":
            if str(release_info.get("version") or "N/A") == str(release_info.get("origin_version") or "N/A") and current_commit == str(release_info.get("origin_commit") or ""):
                return "✅"
            return f"❌ {str(release_info.get('origin_version') or 'N/A')} ({_short_commit(str(release_info.get('origin_commit') or ''))})"
        return "⚠️ version"

    def _build_remote_pbgui_github_status(self, pbremote: PBRemote,
                                          host_state: dict[str, Any]) -> str:
        meta = self._host_meta(host_state)
        server_branch = str(meta.get("pbgb") or "unknown")
        server_commit = str(meta.get("pbgc") or "")
        server_version = str(meta.get("pbgv") or "N/A")
        release_info = self._get_pbgui_release()
        branches = release_info.get("branches") or {}
        if server_branch != "unknown" and server_branch in branches and branches[server_branch]:
            origin_commit = branches[server_branch][0]["full"]
            if server_commit == origin_commit:
                return "✅"
            return f"❌ {server_version} ({_short_commit(origin_commit)})"
        if server_branch == "main":
            if server_version == str(release_info.get("origin_version") or "N/A") and server_commit == str(release_info.get("origin_commit") or ""):
                return "✅"
            return f"❌ {str(release_info.get('origin_version') or 'N/A')} ({_short_commit(str(release_info.get('origin_commit') or ''))})"
        return f"⚠️ {server_version}"

    def _build_remote_pb7_github_status(self, pbremote: PBRemote,
                                        host_state: dict[str, Any]) -> str:
        meta = self._host_meta(host_state)
        server_branch = str(meta.get("pb7b") or "unknown")
        server_commit = str(meta.get("pb7c") or "")
        server_version = str(meta.get("pb7v") or "N/A")
        release_info = self._get_pb7_release()
        branches = release_info.get("branches") or {}
        if server_branch != "unknown" and server_branch in branches and branches[server_branch]:
            origin_commit = branches[server_branch][0]["full"]
            if server_commit == origin_commit:
                return "✅"
            return f"❌ {server_version} ({_short_commit(origin_commit)})"
        if server_branch == "master":
            if server_version == str(release_info.get("origin_version") or "N/A") and server_commit == str(release_info.get("origin_commit") or ""):
                return "✅"
            return f"❌ {str(release_info.get('origin_version') or 'N/A')} ({_short_commit(str(release_info.get('origin_commit') or ''))})"
        return f"⚠️ {server_version}"

    def _build_master_status(self, pbremote: PBRemote, coindata_ok: bool) -> dict[str, Any]:
        summary_row = self._build_master_overview_row(pbremote)
        local_coindata = getattr(pbremote.local_run, "coindata", None)
        local_no_new_privs = _local_no_new_privileges()
        local_sudo_blocked_reason = "Local sudo blocked by runtime (`NoNewPrivs`)." if local_no_new_privs else ""
        pbgui_github = str(summary_row.get("pbgui_github") or "")
        pb7_github = str(summary_row.get("pb7_github") or "")
        return {
            "name": pbremote.name,
            "online": pbremote.is_online(),
            "rclone_ok": bool(pbremote.bucket),
            "coindata_ok": coindata_ok,
            "update_ok": self.vpsmanager.update_status == "successful",
            "update_ready": True,
            "pending_updates": summary_row.get("updates", "N/A"),
            "cmc_credits": getattr(local_coindata, "credits_left", None),
            "last_command": self.vpsmanager.command_text,
            "last_update": self.vpsmanager.last_update,
            "local_sudo_supported": not local_no_new_privs,
            "local_sudo_blocked_reason": local_sudo_blocked_reason,
            "linux_update_supported": not local_no_new_privs,
            "linux_update_blocked_reason": local_sudo_blocked_reason,
            "summary_row": summary_row,
            "pbgui_update_available": pbgui_github.startswith("❌"),
            "pb7_update_available": pb7_github.startswith("❌"),
        }

    def _build_vps_status(self, vps: VPS, host_state: dict[str, Any],
                          pbremote: PBRemote, coindata_ok: bool, *, quick: bool = False) -> dict[str, Any]:
        hostname = str(vps.hostname or "")
        summary_row = self._build_vps_overview_row(pbremote, vps.hostname, host_state)
        live_package_status = None
        if quick:
            # Keep the last full package probe visible between quick pushes.
            cached_package_status = self._vps_package_status_cache.get(hostname) or {}
            live_package_status = cached_package_status.get("data") or None
        else:
            live_package_status = self._get_live_vps_package_status(vps, host_state)
        if live_package_status:
            summary_row = dict(summary_row)
            if live_package_status.get("upgrades") not in (None, ""):
                summary_row["updates"] = live_package_status.get("upgrades")
            summary_row["reboot_required"] = bool(live_package_status.get("reboot", False))
        pbgui_github = self._build_remote_pbgui_github_status(pbremote, host_state)
        pb7_github = self._build_remote_pb7_github_status(pbremote, host_state)
        ssh_online = self._host_online(host_state)
        telemetry_fresh = self._host_telemetry_fresh(host_state)
        telemetry_age = self._host_telemetry_age(host_state)
        if quick:
            if not ssh_online:
                ssh_ok = False
            elif hostname in self._vps_ssh_ok_cache:
                # Keep the last full SSH validation result while the host stays online.
                ssh_ok = bool(self._vps_ssh_ok_cache[hostname])
            else:
                ssh_ok = True
        else:
            ssh_ok = vps.is_vps_ssh_open()
            self._vps_ssh_ok_cache[hostname] = bool(ssh_ok)
        return {
            "hosts_ok": vps.is_vps_in_hosts(),
            "ssh_ok": ssh_ok,
            "init_ok": vps.init_status == "successful",
            "setup_ok": vps.setup_status == "successful",
            "update_ok": vps.update_status == "successful",
            "update_ready": bool(vps.user_pw),
            "pending_updates": summary_row.get("updates", "N/A"),
            "rclone_ok": bool(getattr(pbremote, "bucket", None)),
            "coindata_ok": coindata_ok,
            "cmc_credits": self._host_meta(host_state).get("cmc_credits"),
            "online": ssh_online and telemetry_fresh,
            "ssh_online": ssh_online,
            "telemetry_fresh": telemetry_fresh,
            "telemetry_stale": ssh_online and not telemetry_fresh,
            "telemetry_age": round(telemetry_age, 1) if telemetry_age is not None else None,
            "last_command": vps.command_text,
            "last_update": vps.last_update,
            "last_setup": vps.last_setup,
            "last_init": vps.last_init,
            "install_dir": _install_dir_from_remote_pbgui_dir(vps.remote_pbgui_dir, vps.user),
            "summary_row": summary_row,
            "pbgui_update_available": pbgui_github.startswith("\u274c"),
            "pb7_update_available": pb7_github.startswith("\u274c"),
            "server_metrics": self._build_remote_server_metrics(vps.hostname, host_state),
        }

    def _maybe_auto_sync_api_after_setup(self, vps: VPS) -> None:
        hostname = str(vps.hostname or "").strip()
        last_setup = str(vps.last_setup or "").strip()
        if not hostname or not last_setup or str(vps.setup_status or "") != "successful":
            return
        if self._setup_api_sync_done.get(hostname) == last_setup:
            return
        file_sync_worker = _get_file_sync_worker()
        if file_sync_worker is None:
            return
        entry = file_sync_worker.pool.get_connection(hostname)
        if not entry:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._setup_api_sync_done[hostname] = last_setup

        async def _push_new_host() -> None:
            try:
                results = await file_sync_worker.push_api_keys(hostnames=[hostname], dry_run=False, no_propagate=False)
                result = results.get(hostname) if isinstance(results, dict) else None
                if result and result.get("success"):
                    _log(SERVICE, f"Auto-synced API keys to newly setup VPS {hostname}", level="INFO")
                    return
                self._setup_api_sync_done.pop(hostname, None)
                _log(SERVICE, f"Auto-sync API keys failed for newly setup VPS {hostname}: {result}", level="WARNING")
            except Exception as exc:
                self._setup_api_sync_done.pop(hostname, None)
                _log(SERVICE, f"Auto-sync API keys failed for newly setup VPS {hostname}: {exc}", level="WARNING")

        loop.create_task(_push_new_host())

    def build_vps_status_with_session(self, token: str, hostname: str, *, quick: bool = False) -> dict[str, Any]:
        vps = self._require_vps(hostname)
        self._apply_session_secrets_to_vps(token, vps)
        monitor_state = self._get_monitor_state()
        pbremote = self._ensure_pbremote()
        host_state = self._get_host_telemetry(monitor_state, hostname)
        coindata_ok = bool(self._vps_coindata_status_cache.get(hostname, False)) if quick else False
        if quick:
            self._maybe_auto_sync_api_after_setup(vps)
        return self._build_vps_status(vps, host_state, pbremote, coindata_ok, quick=quick)

    def _get_live_vps_package_status(self, vps: VPS, host_state: dict[str, Any]) -> dict[str, Any] | None:
        hostname = str(vps.hostname or "")
        if not hostname:
            return None
        cached = self._vps_package_status_cache.get(hostname)
        fingerprint = (
            str(vps.command or ""),
            str(vps.update_status or ""),
            str(vps.last_update or ""),
        )
        now = time.time()
        if cached:
            cached_fingerprint = tuple(cached.get("fingerprint") or ())
            age = now - float(cached.get("checked_at") or 0)
            if cached_fingerprint == fingerprint and age < 120:
                return cached.get("data") or None

        if not self._host_online(host_state):
            return (cached or {}).get("data") or None
        if not getattr(vps, "user_pw", None):
            return (cached or {}).get("data") or None
        if _status_running(vps.init_status) or _status_running(vps.setup_status) or _status_running(vps.update_status):
            return (cached or {}).get("data") or None

        live = vps.fetch_package_status()
        if live is None:
            return (cached or {}).get("data") or None
        self._vps_package_status_cache[hostname] = {
            "fingerprint": fingerprint,
            "checked_at": now,
            "data": live,
        }
        return live

    def _build_remote_rtd(self, host_state: dict[str, Any]) -> int:
        system = (host_state or {}).get("system") or {}
        timestamp = _safe_float(system.get("timestamp"), 0.0)
        if not timestamp:
            return 0
        return max(int((time.time() - timestamp) * 1000), 0)

    def _build_remote_server_metrics(self, hostname: str,
                                     host_state: dict[str, Any]) -> dict[str, Any] | None:
        system = (host_state or {}).get("system") or {}
        meta = self._host_meta(host_state)
        if not system:
            return None
        boot = _safe_int(meta.get("boot"))
        last_update = self._host_telemetry_last_update(host_state)
        telemetry_age = self._host_telemetry_age(host_state)
        telemetry_fresh = self._host_telemetry_fresh(host_state)
        return {
            "rtd": self._build_remote_rtd(host_state),
            "boot": datetime.fromtimestamp(boot).strftime("%Y-%m-%d %H:%M:%S") if boot else "",
            "last_update": datetime.fromtimestamp(last_update).strftime("%Y-%m-%d %H:%M:%S") if last_update else "",
            "telemetry_fresh": telemetry_fresh,
            "telemetry_stale": self._host_online(host_state) and not telemetry_fresh,
            "telemetry_age": round(telemetry_age, 1) if telemetry_age is not None else None,
            "cpu": _safe_float(system.get("cpu")),
            "cpu_60s": _safe_float(system.get("cpu_60s")),
            "cpu_60s_window": _safe_float(system.get("cpu_60s_window")),
            "cpu_60s_confirmed": _safe_float(system.get("cpu_60s_window")) >= 60,
            "mem": {
                "total_mb": _safe_int(_safe_float(system.get("mem_total")) / 1024 / 1024),
                "free_mb": _safe_int(_safe_float(system.get("mem_available")) / 1024 / 1024),
                "used_mb": _safe_int(_safe_float(system.get("mem_used")) / 1024 / 1024),
                "usage_pct": _safe_int(system.get("mem_percent")),
                "usage_60s_peak": _safe_float(system.get("mem_60s_peak")),
                "usage_60s_window": _safe_float(system.get("mem_60s_window")),
            },
            "disk": {
                "total_mb": _safe_int(_safe_float(system.get("disk_total")) / 1024 / 1024),
                "used_mb": _safe_int(_safe_float(system.get("disk_used")) / 1024 / 1024),
                "free_mb": _safe_int(_safe_float(system.get("disk_free")) / 1024 / 1024),
                "usage_pct": _safe_int(system.get("disk_percent")),
                "usage_60s_peak": _safe_float(system.get("disk_60s_peak")),
                "usage_60s_window": _safe_float(system.get("disk_60s_window")),
            },
            "swap": {
                "total_mb": _safe_int(_safe_float(system.get("swap_total")) / 1024 / 1024),
                "used_mb": _safe_int(_safe_float(system.get("swap_used")) / 1024 / 1024),
                "free_mb": _safe_int(_safe_float(system.get("swap_free")) / 1024 / 1024),
                "usage_pct": min(_safe_int(system.get("swap_percent")), 100),
                "usage_60s_peak": _safe_float(system.get("swap_60s_peak")),
                "usage_60s_window": _safe_float(system.get("swap_60s_window")),
            },
        }

    def _build_server_metrics(self, server) -> dict[str, Any] | None:
        if not server or not getattr(server, "mem", None) or not getattr(server, "disk", None) or not getattr(server, "swap", None):
            return None
        memory_peak, memory_window = self._update_master_server_metric_peak("memory", _safe_float(server.mem[2]))
        disk_peak, disk_window = self._update_master_server_metric_peak("disk", _safe_float(server.disk[3]))
        swap_total = _safe_float(server.swap[0])
        swap_peak, swap_window = self._update_master_server_metric_peak("swap", _safe_float(server.swap[3]), enabled=swap_total > 0)
        return {
            "rtd": int(getattr(server, "rtd", 0) or 0),
            "boot": datetime.fromtimestamp(server.boot).strftime("%Y-%m-%d %H:%M:%S") if getattr(server, "boot", 0) else "",
            "cpu": _safe_float(server.cpu),
            "cpu_60s": _safe_float(getattr(server, "cpu_60s", 0)),
            "cpu_60s_window": _safe_float(getattr(server, "cpu_60s_window", 0)),
            "cpu_60s_confirmed": _safe_float(getattr(server, "cpu_60s_window", 0)) >= 60,
            "mem": {
                "total_mb": _safe_int(server.mem[0] / 1024 / 1024),
                "free_mb": _safe_int(server.mem[1] / 1024 / 1024),
                "used_mb": _safe_int(server.mem[3] / 1024 / 1024),
                "usage_pct": _safe_int(server.mem[2]),
                "usage_60s_peak": memory_peak,
                "usage_60s_window": memory_window,
            },
            "disk": {
                "total_mb": _safe_int(server.disk[0] / 1024 / 1024),
                "used_mb": _safe_int(server.disk[1] / 1024 / 1024),
                "free_mb": _safe_int(server.disk[2] / 1024 / 1024),
                "usage_pct": _safe_int(server.disk[3]),
                "usage_60s_peak": disk_peak,
                "usage_60s_window": disk_window,
            },
            "swap": {
                "total_mb": _safe_int(server.swap[0] / 1024 / 1024),
                "used_mb": _safe_int(server.swap[1] / 1024 / 1024),
                "free_mb": _safe_int(server.swap[2] / 1024 / 1024),
                "usage_pct": min(_safe_int(server.swap[3]), 100),
                "usage_60s_peak": swap_peak,
                "usage_60s_window": swap_window,
            },
        }

    def _update_master_server_metric_peak(self, metric: str, value: float, *, enabled: bool = True) -> tuple[float, float]:
        metric = str(metric or "").strip().lower()
        history = self._master_server_metric_history.get(metric)
        if history is None:
            return 0.0, 0.0
        if not enabled:
            history.clear()
            return 0.0, 0.0
        now = time.time()
        history.append((now, max(0.0, float(value))))
        cutoff = now - (ROLLING_PEAK_WINDOW_SECONDS + 2.0)
        history[:] = [sample for sample in history if sample[0] >= cutoff]
        if not history:
            return 0.0, 0.0
        peak = round(max(sample[1] for sample in history), 1)
        window = round(now - history[0][0], 1)
        return peak, window

    def _empty_monitor_payload(self) -> dict[str, Any]:
        return {"server": None, "v7": [], "v7_running": [], "multi": [], "single": [], "logfiles": []}

    def _master_bot_cpu_60s(self, name: str, pid: int, ticks: int, now: float) -> tuple[float, float]:
        entry = self._master_bot_cpu_history.get(name)
        if not isinstance(entry, dict) or _safe_int(entry.get("pid")) != pid:
            entry = {"pid": pid, "history": []}
            self._master_bot_cpu_history[name] = entry
        history = entry.get("history")
        if not isinstance(history, list):
            history = []
            entry["history"] = history
        history.append((now, ticks))
        cutoff = now - 62
        history[:] = [sample for sample in history if sample[0] >= cutoff]
        base_sample = None
        for sample in history:
            if now - sample[0] >= 60:
                base_sample = sample
            else:
                break
        if base_sample is not None:
            dt_sec = now - base_sample[0]
            if dt_sec > 0:
                return round((ticks - base_sample[1]) / (dt_sec * 100), 2), round(dt_sec, 1)
        if history:
            return 0.0, round(now - history[0][0], 1)
        return 0.0, 0.0

    def _collect_local_master_live_bot_stats(self) -> dict[str, dict[str, float]]:
        stats: dict[str, dict[str, float]] = {}
        now = time.time()
        seen_names: set[str] = set()
        try:
            result = subprocess.run(
                ["ps", "auxw"],
                capture_output=True,
                text=True,
                timeout=5,
                cwd=PBGDIR,
            )
        except Exception as exc:
            _log(SERVICE, f"local master ps probe failed: {exc}", level="WARNING")
            return stats
        if result.returncode != 0:
            return stats
        for raw_line in (result.stdout or "").splitlines():
            if "main.py" not in raw_line or "config_run.json" not in raw_line:
                continue
            parts = raw_line.split(None, 10)
            if len(parts) < 11:
                continue
            try:
                pid = int(parts[1])
            except Exception:
                continue
            cmdline = parts[10]
            bot_name = ""
            try:
                for arg in shlex.split(cmdline):
                    if arg.endswith("/config_run.json") or arg.endswith("\\config_run.json"):
                        bot_name = Path(arg).parent.name
                        break
            except Exception:
                continue
            if not bot_name:
                continue
            seen_names.add(bot_name)
            cpu_60s = 0.0
            cpu_60s_window = 0.0
            try:
                stat_path = Path(f"/proc/{pid}/stat")
                if stat_path.exists():
                    stat_parts = stat_path.read_text(encoding="utf-8", errors="ignore").split()
                    ticks = _safe_int(stat_parts[13]) + _safe_int(stat_parts[14])
                    cpu_60s, cpu_60s_window = self._master_bot_cpu_60s(bot_name, pid, ticks, now)
            except Exception:
                cpu_60s = 0.0
                cpu_60s_window = 0.0
            swap_mb = 0.0
            try:
                status_path = Path(f"/proc/{pid}/status")
                if status_path.exists():
                    for line in status_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                        if line.startswith("VmSwap:"):
                            swap_mb = round(_safe_float(line.split()[1]) / 1024, 2)
                            break
            except Exception:
                swap_mb = 0.0
            stats[bot_name] = {
                "cpu": round(_safe_float(parts[2]), 2),
                "cpu_60s": cpu_60s,
                "cpu_60s_window": cpu_60s_window,
                "rss_mb": round(_safe_float(parts[5]) / 1024, 2),
                "swap_mb": swap_mb,
            }
        for name in list(self._master_bot_cpu_history.keys()):
            if name not in seen_names:
                self._master_bot_cpu_history.pop(name, None)
        return stats

    def _collect_local_master_monitor_snapshot(self) -> dict[str, Any]:
        env = dict(os.environ)
        env.update({
            "PBGUI_CACHE_VERSION": str(MONITOR_CACHE_VERSION),
            "PBGUI_CACHE": json.dumps(self._master_monitor_cache),
        })
        try:
            result = subprocess.run(
                INSTANCE_COLLECT_SCRIPT,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30,
                cwd=PBGDIR,
                env=env,
            )
        except Exception as exc:
            _log(SERVICE, f"local master monitor collect failed: {exc}", level="WARNING")
            return {"monitors": [], "v7": [], "bot_logs": {}}
        if result.returncode != 0 or not result.stdout:
            stderr = str(result.stderr or "").strip()
            if stderr:
                _log(SERVICE, f"local master monitor collect failed: {stderr}", level="WARNING")
            return {"monitors": [], "v7": [], "bot_logs": {}}
        try:
            parsed = json.loads(result.stdout.strip())
        except json.JSONDecodeError as exc:
            _log(SERVICE, f"local master monitor JSON parse failed: {exc}", level="WARNING")
            return {"monitors": [], "v7": [], "bot_logs": {}}
        if not isinstance(parsed, dict):
            return {"monitors": [], "v7": [], "bot_logs": {}}
        new_cache = parsed.get("cache")
        if isinstance(new_cache, dict):
            self._master_monitor_cache = new_cache
        return {
            "monitors": parsed.get("monitors") if isinstance(parsed.get("monitors"), list) else [],
            "v7": parsed.get("v7") if isinstance(parsed.get("v7"), list) else [],
            "bot_logs": parsed.get("bot_logs") if isinstance(parsed.get("bot_logs"), dict) else {},
        }

    def _bot_count_total(self, hostname: str, bot_name: str, metric: str) -> int:
        monitor = get_monitor()
        if not monitor or not hostname or not bot_name:
            return 0
        try:
            payload = monitor.get_bot_metric_history(hostname, bot_name, metric)
        except Exception:
            return 0
        return _safe_int((payload or {}).get("total_count"))

    def _bot_pnl_total(self, hostname: str, bot_name: str) -> tuple[float, int]:
        monitor = get_monitor()
        if not monitor or not hostname or not bot_name:
            return 0.0, 0
        try:
            payload = monitor.get_bot_metric_history(hostname, bot_name, "pnl")
        except Exception:
            return 0.0, 0
        return _safe_float((payload or {}).get("total_pnl")), _safe_int((payload or {}).get("total_fills"))

    def _build_local_running_v7_payload(self, v7_rows: list[dict[str, Any]], existing_names: set[str] | None = None) -> list[dict[str, Any]]:
        known_names = existing_names or set()
        items: list[dict[str, Any]] = []
        for instance in v7_rows:
            if not _truthy(instance.get("running", True)):
                continue
            name = str(instance.get("name") or "")
            if not name or name in known_names:
                continue
            items.append(
                {
                    "name": name,
                    "version": _safe_int(instance.get("cv")),
                    "enabled_on": str(instance.get("eo") or ""),
                    "activate_ts": "",
                }
            )
        items.sort(key=lambda item: item["name"])
        return items

    def _build_local_master_monitor_payload(self, pbremote: PBRemote, *, refresh: bool) -> dict[str, Any]:
        if not refresh and self._master_monitor_payload_cache is not None:
            return self._master_monitor_payload_cache
        payload = self._empty_monitor_payload()
        payload["server"] = self._build_server_metrics(pbremote)
        snapshot = self._collect_local_master_monitor_snapshot()
        live_stats = self._collect_local_master_live_bot_stats()
        cfg = self.monitor_config
        for monitor in snapshot.get("monitors") or []:
            name = str(monitor.get("u") or "")
            live = live_stats.get(name) or {}
            start_ts = _safe_int(monitor.get("st"))
            pnl_hist_total, pnls_hist_total = self._bot_pnl_total(pbremote.name, name)
            item = {
                "server": pbremote.name,
                "version": str(self._get_pb7_release().get("version") or "N/A"),
                "name": name,
                "pb_version": "7",
                "start_time": datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S") if start_ts else "",
                "memory_mb": round(_safe_float(live.get("rss_mb")), 2),
                "swap_mb": round(_safe_float(live.get("swap_mb")), 2),
                "cpu": round(_safe_float(live.get("cpu")), 2),
                "cpu_60s": round(_safe_float(live.get("cpu_60s")), 2),
                "cpu_60s_window": round(_safe_float(live.get("cpu_60s_window")), 1),
                "cpu_60s_confirmed": _safe_float(live.get("cpu_60s_window")) >= 60,
                "pnls_today": _safe_int(monitor.get("ct")),
                "pnl_today": _safe_float(monitor.get("pt")),
                "pnls_hist_total": pnls_hist_total,
                "pnl_hist_total": pnl_hist_total,
                "errors_today": _safe_int(monitor.get("et")),
                "errors_4w": self._bot_count_total(pbremote.name, name, "errors"),
                "tracebacks_today": _safe_int(monitor.get("tt")),
                "tracebacks_4w": self._bot_count_total(pbremote.name, name, "tracebacks"),
            }
            item["levels"] = {
                "cpu": _metric_level(item["cpu"], cfg.cpu_warning_v7, cfg.cpu_error_v7),
                "memory": _metric_level(item["memory_mb"], cfg.mem_warning_v7, cfg.mem_error_v7),
                "swap": _metric_level(item["swap_mb"], cfg.swap_warning_v7, cfg.swap_error_v7),
                "errors": _metric_level(item["errors_today"], cfg.error_warning_v7, cfg.error_error_v7),
                "tracebacks": _metric_level(item["tracebacks_today"], cfg.traceback_warning_v7, cfg.traceback_error_v7),
            }
            payload["v7"].append(item)
            if name:
                payload["logfiles"].append(f"run_v7/{name}/passivbot.log")
        existing_v7_names = {item["name"] for item in payload["v7"] if item.get("name")}
        payload["v7_running"] = self._build_local_running_v7_payload(snapshot.get("v7") or [], existing_v7_names)
        for item in payload["v7_running"]:
            if item.get("name"):
                payload["logfiles"].append(f"run_v7/{item['name']}/passivbot.log")
        payload["logfiles"] = sorted(dict.fromkeys(payload["logfiles"]))
        self._master_monitor_payload_cache = payload
        return payload

    def _build_master_pbgui_branch_state(self, pbremote: PBRemote) -> dict[str, Any]:
        local_run = pbremote.local_run
        release_info = self._get_pbgui_release()
        current_branch = str(release_info.get("current_branch") or "unknown")
        current_commit = str(release_info.get("current_commit") or "")
        return {
            "current_branch": current_branch,
            "current_commit": current_commit,
            "branches": release_info.get("branches") or {},
        }

    def _build_master_pb7_branch_state(self, pbremote: PBRemote) -> dict[str, Any]:
        repo_dir = _configured_pb7dir()
        release_info = self._get_pb7_release()
        current_branch = str(release_info.get("current_branch") or "unknown")
        current_commit = str(release_info.get("current_commit") or "")
        branches = release_info.get("branches") or {}
        known_remotes = list_git_remotes(repo_dir) if repo_dir else []
        for opt in ("origin", "fork"):
            if opt not in known_remotes:
                known_remotes.append(opt)
        remote_urls = {name: get_git_remote_url(repo_dir, name) for name in known_remotes if repo_dir}
        tracking_remote_name = get_git_branch_remote(repo_dir, current_branch or "") if repo_dir else ""
        branch_tracking_remotes = get_git_branch_remotes(repo_dir, list(branches.keys())) if repo_dir else {}
        default_remote_name = tracking_remote_name if tracking_remote_name in known_remotes else ("fork" if "fork" in known_remotes else ("origin" if "origin" in known_remotes else (known_remotes[0] if known_remotes else "")))
        return {
            "current_branch": current_branch,
            "current_commit": current_commit,
            "branches": branches,
            "known_remotes": known_remotes,
            "remote_urls": remote_urls,
            "branch_tracking_remotes": branch_tracking_remotes,
            "default_remote_name": default_remote_name,
            "upstream_remote_name": PB7_UPSTREAM_REMOTE_NAME,
            "upstream_remote_url": PB7_UPSTREAM_REMOTE_URL,
        }

    def _build_vps_pbgui_branch_state(self, pbremote: PBRemote,
                                      host_state: dict[str, Any]) -> dict[str, Any]:
        meta = self._host_meta(host_state)
        return {
            "current_branch": str(meta.get("pbgb") or "unknown"),
            "current_commit": str(meta.get("pbgc") or ""),
            "branches": self._get_pbgui_release().get("branches") or {},
        }

    def _build_vps_pb7_branch_state(self, pbremote: PBRemote,
                                    host_state: dict[str, Any],
                                    hostname: str) -> dict[str, Any]:
        meta = self._host_meta(host_state)
        repo_dir = _configured_pb7dir()
        branches = self._get_pb7_release().get("branches") or {}
        known_remotes = list_git_remotes(repo_dir) if repo_dir else []
        for opt in ("origin", "fork"):
            if opt not in known_remotes:
                known_remotes.append(opt)
        remote_urls = {name: get_git_remote_url(repo_dir, name) for name in known_remotes if repo_dir}
        current_branch = str(meta.get("pb7b") or "unknown")
        tracking_remote_name = get_git_branch_remote(repo_dir, current_branch or "") if repo_dir else ""
        branch_tracking_remotes = get_git_branch_remotes(repo_dir, list(branches.keys())) if repo_dir else {}
        default_remote_name = tracking_remote_name if tracking_remote_name in known_remotes else ("origin" if "origin" in known_remotes else (known_remotes[0] if known_remotes else ""))
        return {
            "hostname": hostname,
            "current_branch": current_branch,
            "current_commit": str(meta.get("pb7c") or ""),
            "branches": branches,
            "known_remotes": known_remotes,
            "remote_urls": remote_urls,
            "branch_tracking_remotes": branch_tracking_remotes,
            "default_remote_name": default_remote_name,
            "upstream_remote_name": PB7_UPSTREAM_REMOTE_NAME,
            "upstream_remote_url": PB7_UPSTREAM_REMOTE_URL,
        }

    def _build_master_progress(self, *, include_log: bool = False) -> dict[str, Any]:
        return {
            "command": self.vpsmanager.command,
            "command_text": self.vpsmanager.command_text,
            "status": self.vpsmanager.update_status,
            "last_update": self.vpsmanager.last_update,
            "log": self.vpsmanager.get_update_log_text() if include_log else "",
        }

    def _build_vps_progress(self, vps: VPS, *, include_logs: bool = False) -> dict[str, Any]:
        return {
            "hostname": vps.hostname,
            "command": vps.command,
            "command_text": vps.command_text,
            "init_status": vps.init_status,
            "setup_status": vps.setup_status,
            "update_status": vps.update_status,
            "last_init": vps.last_init,
            "last_setup": vps.last_setup,
            "last_update": vps.last_update,
            "init_log": vps.get_init_log_text() if include_logs else "",
            "setup_log": vps.get_setup_log_text() if include_logs else "",
            "update_log": vps.get_update_log_text() if include_logs else "",
        }

    def _build_vps_config(self, token: str, vps: VPS) -> dict[str, Any]:
        secret_status = self._session_secret_meta(token, str(vps.hostname or ""))
        # Keep detail/config payloads secret-free. The frontend only gets
        # presence/TTL metadata and must explicitly request an on-demand reveal.
        return {
            "hostname": vps.hostname,
            "ip": vps.ip or "",
            "user": vps.user or "",
            "swap": vps.swap or "0",
            "bucket": vps.bucket or "",
            "coinmarketcap_api_key": vps.coinmarketcap_api_key or "",
            "firewall": bool(vps.firewall),
            "firewall_ssh_port": int(vps.firewall_ssh_port or 22),
            "firewall_ssh_ips": vps.firewall_ssh_ips or "",
            "init_methode": vps.init_methode or "root",
            "remove_user": bool(vps.remove_user),
            "secret_status": secret_status,
        }

    def _default_vps_logging_limits_mb(self) -> dict[str, float]:
        limits = {service: float(VPS_LOGGING_DEFAULT_MB) for service in VPS_LOGGING_SERVICES}
        limits["vps_cleanup"] = float(VPS_LOGGING_CLEANUP_MB)
        return limits

    def _default_vps_deploy_settings(self) -> dict[str, Any]:
        return {
            "action": VPS_DEPLOY_DEFAULT_ACTION,
            "action_text": _vps_deploy_command_text(VPS_DEPLOY_DEFAULT_ACTION),
            "mode": VPS_DEPLOY_DEFAULT_MODE,
            "debug": False,
            "reboot_requested": False,
            "selected_hosts": [],
            "actions": [
                {
                    "command": command,
                    "command_text": _vps_deploy_command_text(command),
                }
                for command in VPS_DEPLOY_ACTIONS
            ],
            "modes": list(VPS_DEPLOY_MODES),
        }

    def _vps_deploy_extra_vars(self, command: str, settings: dict[str, Any] | None = None) -> dict[str, Any] | None:
        normalized = _normalize_vps_deploy_command(command)
        if normalized == COMMAND_VPS_DEPLOY_LOGGING:
            return {
                "vps_logging_services": self.get_vps_logging_config().get("services") or [],
            }
        if normalized == COMMAND_VPS_UPDATE:
            reboot_requested = bool((settings or {}).get("reboot_requested"))
            return {
                "reboot": reboot_requested,
                "reboot_requested": reboot_requested,
            }
        return None

    def _normalize_vps_deploy_host_logs(
        self,
        item: dict[str, Any],
        *,
        command: str,
        legacy_host_offsets: dict[str, int] | None = None,
    ) -> dict[str, dict[str, Any]]:
        hostnames = [str(host).strip() for host in item.get("hostnames") or [] if str(host).strip()]
        raw_host_logs = item.get("host_logs") if isinstance(item.get("host_logs"), dict) else {}
        host_logs: dict[str, dict[str, Any]] = {}
        offsets = legacy_host_offsets if legacy_host_offsets is not None else {}
        is_legacy_entry = not str(item.get("id") or "").strip() and item.get("host_count") is None and item.get("service_count") is None
        for hostname, payload in raw_host_logs.items():
            clean_host = str(hostname or "").strip()
            if not clean_host or not isinstance(payload, dict):
                continue
            entry_command = _normalize_vps_deploy_command(payload.get("command") or item.get("command") or command)
            host_logs[clean_host] = {
                "command": entry_command,
                "command_text": str(payload.get("command_text") or _vps_deploy_command_text(entry_command)),
                "started_at": str(payload.get("started_at") or item.get("started_at") or ""),
                "run_id": str(payload.get("run_id") or ""),
                "filename": str(payload.get("filename") or ""),
                "file_alias": str(payload.get("file_alias") or ""),
            }
        for hostname in hostnames:
            if hostname in host_logs and host_logs[hostname].get("file_alias"):
                continue
            if not is_legacy_entry:
                continue
            host_offset = int(offsets.get(hostname, 0) or 0)
            entry_command = _normalize_vps_deploy_command(item.get("command") or command)
            action = entry_command if host_offset <= 0 else f"{entry_command}.{host_offset}"
            filename = f"{entry_command}.log" if host_offset <= 0 else f"{entry_command}.log.{host_offset}"
            host_logs[hostname] = {
                "command": entry_command,
                "command_text": _vps_deploy_command_text(entry_command),
                "started_at": str(item.get("started_at") or ""),
                "run_id": "",
                "filename": filename,
                "file_alias": f"VPSAction:{hostname}:{action}",
            }
            offsets[hostname] = host_offset + 1
        return host_logs

    def _record_vps_deploy(
        self,
        *,
        command: str,
        mode: str,
        hostnames: list[str],
        host_logs: dict[str, dict[str, Any]],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_command = _normalize_vps_deploy_command(command)
        normalized_mode = _normalize_vps_deploy_mode(mode)
        clean_options = dict(options or {})
        entry = {
            "id": f"vps-deploy-{int(time.time() * 1000)}",
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "command": normalized_command,
            "command_text": _vps_deploy_command_text(normalized_command),
            "mode": normalized_mode,
            "hostnames": list(hostnames),
            "options": clean_options,
            "services": [str((item or {}).get("service") or "").strip() for item in (self.get_vps_logging_config().get("services") or []) if str((item or {}).get("service") or "").strip()] if normalized_command == COMMAND_VPS_DEPLOY_LOGGING else [],
            "host_logs": host_logs,
        }
        with self._deploy_history_lock:
            history = _load_json_list(VPS_DEPLOY_HISTORY_FILE)
            history.insert(0, entry)
            history = history[:VPS_DEPLOY_HISTORY_LIMIT]
            _atomic_write_json(VPS_DEPLOY_HISTORY_FILE, history)
        return {
            "id": str(entry["id"]),
            "started_at": str(entry["started_at"]),
            "command": str(entry["command"]),
            "command_text": str(entry["command_text"]),
            "mode": str(entry["mode"]),
            "options": dict(entry["options"]),
            "hostnames": list(entry["hostnames"]),
            "services": list(entry["services"]),
            "host_logs": dict(entry["host_logs"]),
            "host_count": len(entry["hostnames"]),
            "service_count": len(entry["services"]),
        }

    def get_vps_deploy_settings(self) -> dict[str, Any]:
        section = load_ini_section(VPS_DEPLOY_SECTION)
        defaults = self._default_vps_deploy_settings()
        selected_hosts = [item.strip() for item in str(section.get("selected_hosts", "")).split(",") if item.strip()]
        action = _normalize_vps_deploy_command(section.get("action"))
        mode = _normalize_vps_deploy_mode(section.get("mode"))
        debug = _truthy(section.get("debug"))
        reboot_requested = _truthy(section.get("reboot_requested"))
        return {
            "action": action,
            "action_text": _vps_deploy_command_text(action),
            "mode": mode,
            "debug": debug,
            "reboot_requested": reboot_requested,
            "selected_hosts": selected_hosts,
            "actions": defaults["actions"],
            "modes": defaults["modes"],
        }

    def save_vps_deploy_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        incoming = payload if isinstance(payload, dict) else {}
        selected_hosts: list[str] = []
        seen_hosts: set[str] = set()
        for raw_host in incoming.get("selected_hosts") or []:
            hostname = str(raw_host or "").strip()
            if not hostname or hostname in seen_hosts:
                continue
            self._require_vps(hostname)
            seen_hosts.add(hostname)
            selected_hosts.append(hostname)
        values = {
            "action": _normalize_vps_deploy_command(incoming.get("action")),
            "mode": _normalize_vps_deploy_mode(incoming.get("mode")),
            "debug": "1" if _truthy(incoming.get("debug")) else "0",
            "reboot_requested": "1" if _truthy(incoming.get("reboot_requested")) else "0",
            "selected_hosts": ",".join(selected_hosts),
        }
        save_ini_section(VPS_DEPLOY_SECTION, values)
        return self.get_vps_deploy_settings()

    def get_vps_deploy_history(self) -> list[dict[str, Any]]:
        history = _load_json_list(VPS_DEPLOY_HISTORY_FILE)
        cleaned: list[dict[str, Any]] = []
        legacy_host_offsets: dict[str, int] = {}
        for item in history[:VPS_DEPLOY_HISTORY_LIMIT]:
            command = _normalize_vps_deploy_command(item.get("command"))
            hostnames = [str(host).strip() for host in item.get("hostnames") or [] if str(host).strip()]
            services = [str(service).strip() for service in item.get("services") or [] if str(service).strip()]
            cleaned.append({
                "id": str(item.get("id") or ""),
                "started_at": str(item.get("started_at") or ""),
                "command": command,
                "command_text": str(item.get("command_text") or _vps_deploy_command_text(command)),
                "mode": _normalize_vps_deploy_mode(item.get("mode")),
                "options": dict(item.get("options") or {}),
                "hostnames": hostnames,
                "services": services,
                "host_logs": self._normalize_vps_deploy_host_logs(item, command=command, legacy_host_offsets=legacy_host_offsets),
                "host_count": len(hostnames),
                "service_count": len(services),
            })
        return cleaned

    def _wait_for_vps_command_finish(self, hostname: str, run_id: str, *, timeout_seconds: int = 3600) -> None:
        target_run_id = str(run_id or "").strip()
        if not target_run_id:
            raise ValueError(f"Missing deploy run id for {hostname}.")
        deadline = time.time() + max(1, int(timeout_seconds))
        run_appear_deadline = time.time() + DEPLOY_RUN_APPEAR_TIMEOUT_SECONDS
        while time.time() < deadline:
            vps = self._require_vps(hostname)
            current_run_id = str(getattr(vps, "command_run_id", "") or "")
            current_status = str(getattr(vps, "update_status", "") or "")
            if current_run_id != target_run_id:
                if time.time() >= run_appear_deadline:
                    raise TimeoutError(f"Timed out waiting for {hostname} to start deploy run {target_run_id}.")
                time.sleep(0.5)
                continue
            if current_status in {"successful", "failed", "error", "timeout", "canceled", "cancelled"}:
                return
            time.sleep(1)
        raise TimeoutError(f"Timed out waiting for {hostname} to finish {target_run_id}.")

    def _deploy_entry_options(self, command: str, extra_vars: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized_command = _normalize_vps_deploy_command(command)
        options: dict[str, Any] = {}
        merged_extra_vars = dict(extra_vars or {})
        if normalized_command == COMMAND_VPS_UPDATE:
            reboot_requested = bool(merged_extra_vars.get("reboot_requested") or merged_extra_vars.get("reboot"))
            options["reboot_requested"] = reboot_requested
        return options

    def _get_vps_deploy_entry(self, entry_id: str) -> dict[str, Any]:
        target_id = str(entry_id or "").strip()
        if not target_id:
            raise ValueError("Deploy session id is required.")
        for item in self.get_vps_deploy_history():
            if str(item.get("id") or "") == target_id:
                return item
        raise ValueError("Deploy session not found.")

    def _update_vps_deploy_entry(self, entry_id: str, *, host_logs: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
        target_id = str(entry_id or "").strip()
        if not target_id:
            raise ValueError("Deploy session id is required.")
        with self._deploy_history_lock:
            history = _load_json_list(VPS_DEPLOY_HISTORY_FILE)
            updated = False
            for index, item in enumerate(history):
                if str(item.get("id") or "") != target_id:
                    continue
                next_item = dict(item)
                merged_host_logs = dict(next_item.get("host_logs") or {})
                for hostname, payload in (host_logs or {}).items():
                    clean_host = str(hostname or "").strip()
                    if not clean_host or not isinstance(payload, dict):
                        continue
                    merged_host_logs[clean_host] = dict(payload)
                next_item["host_logs"] = merged_host_logs
                history[index] = next_item
                updated = True
                break
            if not updated:
                raise ValueError("Deploy session not found.")
            _atomic_write_json(VPS_DEPLOY_HISTORY_FILE, history[:VPS_DEPLOY_HISTORY_LIMIT])
        return self._get_vps_deploy_entry(target_id)

    def _start_vps_deploy_host(
        self,
        token: str,
        *,
        hostname: str,
        command: str,
        debug: bool,
        extra_vars: dict[str, Any] | None,
    ) -> dict[str, Any]:
        normalized_command = _normalize_vps_deploy_command(command)
        vps = self._require_vps(hostname)
        self._apply_session_secrets_to_vps(token, vps)
        if _vps_deploy_requires_user_password(normalized_command) and not getattr(vps, "user_pw", None):
            raise ValueError(f"VPS user password missing for {hostname}.")
        vps.command = normalized_command
        vps.command_text = _vps_deploy_command_text(normalized_command)
        self.vpsmanager.update_vps(vps, debug=debug, extra_vars=extra_vars or None)
        task_log_name = vps._task_log_path(vps.command, COMMAND_VPS_UPDATE).name
        return {
            "command": normalized_command,
            "command_text": _vps_deploy_command_text(normalized_command),
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "run_id": str(vps.command_run_id or ""),
            "filename": task_log_name,
            "file_alias": f"VPSAction:{hostname}:{task_log_name}",
        }

    def _ensure_vps_deploy_session_worker(self, entry_id: str) -> None:
        target_id = str(entry_id or "").strip()
        if not target_id:
            return
        thread: threading.Thread | None = None
        with self._deploy_sessions_lock:
            session = self._deploy_sessions.get(target_id)
            if not session:
                return
            thread = session.get("worker")
            if thread is not None and thread.is_alive():
                return
            thread = threading.Thread(
                target=self._run_vps_deploy_session_worker,
                kwargs={"entry_id": target_id},
                daemon=True,
                name=f"vps-deploy-session-{target_id}",
            )
            session["worker"] = thread
        thread.start()

    def _run_vps_deploy_session_worker(self, *, entry_id: str) -> None:
        target_id = str(entry_id or "").strip()
        while True:
            with self._deploy_sessions_lock:
                session = self._deploy_sessions.get(target_id)
                if not session:
                    return
                current_host = str(session.get("current_host") or "")
                current_run_id = str(session.get("current_run_id") or "")
                wake_event = session.get("wake_event")
            if current_host and current_run_id:
                try:
                    self._wait_for_vps_command_finish(current_host, current_run_id)
                except Exception as exc:
                    _log(SERVICE, f"deploy session wait failed for {current_host}: {exc}", level="WARNING")
                finally:
                    with self._deploy_sessions_lock:
                        session = self._deploy_sessions.get(target_id)
                        if session and str(session.get("current_host") or "") == current_host and str(session.get("current_run_id") or "") == current_run_id:
                            session["current_host"] = ""
                            session["current_run_id"] = ""
                continue

            next_host = ""
            session_snapshot: dict[str, Any] | None = None
            with self._deploy_sessions_lock:
                session = self._deploy_sessions.get(target_id)
                if not session:
                    return
                pending = session.get("pending") or []
                if pending:
                    next_host = str(pending.pop(0) or "").strip()
                    session["pending"] = pending
                    session_snapshot = {
                        "token": str(session.get("token") or ""),
                        "command": str(session.get("command") or ""),
                        "debug": bool(session.get("debug")),
                        "extra_vars": dict(session.get("extra_vars") or {}),
                    }
                elif session.get("finalized"):
                    self._deploy_sessions.pop(target_id, None)
                    return
                elif isinstance(wake_event, threading.Event):
                    wake_event.clear()
            if not next_host:
                if isinstance(wake_event, threading.Event):
                    wake_event.wait(timeout=0.5)
                else:
                    time.sleep(0.5)
                continue
            if not session_snapshot:
                continue
            try:
                host_log = self._start_vps_deploy_host(
                    str(session_snapshot.get("token") or ""),
                    hostname=next_host,
                    command=str(session_snapshot.get("command") or ""),
                    debug=bool(session_snapshot.get("debug")),
                    extra_vars=dict(session_snapshot.get("extra_vars") or {}),
                )
                self._update_vps_deploy_entry(target_id, host_logs={next_host: host_log})
                with self._deploy_sessions_lock:
                    session = self._deploy_sessions.get(target_id)
                    if session:
                        started = set(session.get("started") or set())
                        started.add(next_host)
                        session["started"] = started
                        session["current_host"] = next_host
                        session["current_run_id"] = str(host_log.get("run_id") or "")
            except Exception as exc:
                _log(SERVICE, f"deploy session failed to start {next_host}: {exc}", level="WARNING")

    def _validate_vps_user_password(
        self,
        hostname: str,
        password: str,
        *,
        timeout: int = 10,
        accept_unknown_host: bool = False,
    ) -> None:
        password_value = str(password or "")
        if not password_value:
            raise ValueError("VPS sudo password is required.")
        vps = self._require_vps(hostname)
        if not str(vps.ip or "").strip() or not str(vps.user or "").strip():
            raise ValueError(f"Cannot validate {hostname}: missing SSH host or username.")
        ssh_host = str(getattr(vps, "hostname", "") or "").strip() or str(vps.ip or "").strip()
        use_private_key = bool(
            str(getattr(vps, "init_methode", "") or "").strip() == "private_key"
            and str(getattr(vps, "private_key_file", "") or "").strip()
        )
        import paramiko

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy() if accept_unknown_host else paramiko.RejectPolicy())
        try:
            connect_kwargs = {
                "hostname": ssh_host,
                "username": vps.user,
                "timeout": timeout,
                "banner_timeout": timeout,
                "auth_timeout": timeout,
                "allow_agent": False,
                "look_for_keys": False,
            }
            if use_private_key:
                connect_kwargs["key_filename"] = str(vps.private_key_file)
            else:
                connect_kwargs["password"] = password_value
            ssh.connect(**connect_kwargs)
            stdin, stdout, stderr = ssh.exec_command("sudo -k -S -p '' -v", timeout=timeout, get_pty=False)
            stdin.write(password_value + "\n")
            stdin.flush()
            stdout_text = stdout.read().decode(errors="ignore")
            stderr_text = stderr.read().decode(errors="ignore")
            exit_status = stdout.channel.recv_exit_status()
            combined = "\n".join([stdout_text, stderr_text]).strip()
            lowered = combined.lower()
            wrong_password_markers = (
                "incorrect password",
                "sorry, try again",
                "no password was provided",
                "a password is required",
                "1 incorrect password attempt",
            )
            if any(marker in lowered for marker in wrong_password_markers):
                raise ValueError(f"Incorrect VPS sudo password for {hostname}.")
            if exit_status != 0:
                detail = combined.splitlines()[0].strip() if combined else "sudo -v failed"
                raise ValueError(f"Failed to validate sudo access on {hostname}: {detail}")
        except paramiko.AuthenticationException:
            if use_private_key:
                raise ValueError(f"Cannot connect to {hostname} via SSH with the configured private key.")
            raise ValueError(f"Cannot connect to {hostname} via SSH with the supplied password.")
        except paramiko.SSHException as exc:
            if not accept_unknown_host and "not found in known_hosts" in str(exc):
                raise UnknownHostKeyError(hostname=hostname, ssh_host=ssh_host, ip=str(vps.ip or "")) from exc
            raise ValueError(f"Failed to validate sudo password on {hostname}: {exc}") from exc
        except ValueError:
            raise
        except Exception as exc:
            raise ValueError(f"Failed to validate sudo password on {hostname}: {exc}") from exc
        finally:
            try:
                ssh.close()
            except Exception:
                pass

    def _run_vps_deploy_batch(
        self,
        *,
        token: str,
        command: str,
        mode: str,
        hostnames: list[str],
        debug: bool,
        extra_vars: dict[str, Any] | None,
        entry_id: str,
    ) -> None:
        try:
            if mode == "sequential":
                for hostname in hostnames:
                    host_log = self._start_vps_deploy_host(
                        token,
                        hostname=hostname,
                        command=command,
                        debug=debug,
                        extra_vars=extra_vars,
                    )
                    self._update_vps_deploy_entry(entry_id, host_logs={hostname: host_log})
                    run_id = str(host_log.get("run_id") or "").strip()
                    self._wait_for_vps_command_finish(hostname, run_id)
        except Exception as exc:
            _log(SERVICE, f"vps deploy batch failed for {command}: {exc}", level="WARNING")
        finally:
            self._deploy_threads.pop(entry_id, None)

    def get_vps_logging_config(self) -> dict[str, Any]:
        section = load_ini_section("vps_logging")
        default_limits = self._default_vps_logging_limits_mb()
        services: list[dict[str, Any]] = []
        for service in VPS_LOGGING_SERVICES:
            default_mb = float(default_limits.get(service, VPS_LOGGING_DEFAULT_MB))
            raw_value = section.get(f"{service.lower()}_max_mb", "")
            max_mb = _safe_float_str(raw_value, default_mb)
            if max_mb <= 0:
                max_mb = default_mb
            services.append({
                "service": service,
                "max_mb": round(max_mb, 4),
                "default_max_mb": round(default_mb, 4),
                "backup_count": 0,
            })
        return {
            "services": services,
            "selected_hosts": [item.strip() for item in str(section.get("selected_hosts", "")).split(",") if item.strip()],
        }

    def save_vps_logging_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        incoming = payload if isinstance(payload, dict) else {}
        default_limits = self._default_vps_logging_limits_mb()
        selected_hosts: list[str] = []
        seen_hosts: set[str] = set()
        for raw_host in incoming.get("selected_hosts") or []:
            hostname = str(raw_host or "").strip()
            if not hostname or hostname in seen_hosts:
                continue
            self._require_vps(hostname)
            seen_hosts.add(hostname)
            selected_hosts.append(hostname)
        service_values: dict[str, str] = {"selected_hosts": ",".join(selected_hosts)}
        incoming_services = incoming.get("services") or []
        by_name = {
            str((item or {}).get("service") or "").strip(): item
            for item in incoming_services
            if isinstance(item, dict)
        }
        for service in VPS_LOGGING_SERVICES:
            default_mb = float(default_limits.get(service, VPS_LOGGING_DEFAULT_MB))
            raw_value = ((by_name.get(service) or {}).get("max_mb"))
            max_mb = _safe_float_str(raw_value, default_mb)
            if max_mb <= 0:
                raise ValueError(f"{service} max_mb must be greater than 0.")
            service_values[f"{service.lower()}_max_mb"] = str(round(max_mb, 4)).rstrip("0").rstrip(".")
        save_ini_section("vps_logging", service_values)
        return self.get_vps_logging_config()

    def get_vps_logging_deploy_history(self) -> list[dict[str, Any]]:
        return [item for item in self.get_vps_deploy_history() if str(item.get("command") or "") == COMMAND_VPS_DEPLOY_LOGGING]

    def _record_vps_logging_deploy(self, hostnames: list[str], services: list[dict[str, Any]], host_logs: dict[str, dict[str, Any]]) -> dict[str, Any]:
        return self._record_vps_deploy(
            command=COMMAND_VPS_DEPLOY_LOGGING,
            mode=VPS_DEPLOY_DEFAULT_MODE,
            hostnames=hostnames,
            host_logs=host_logs,
            options={},
        )

    def run_vps_deploy(
        self,
        token: str,
        hostnames: list[Any],
        *,
        command: str,
        mode: str,
        debug: bool = False,
        extra_vars: dict[str, Any] | None = None,
        record_history: bool = True,
    ) -> dict[str, Any]:
        normalized_command = _normalize_vps_deploy_command(command)
        normalized_mode = _normalize_vps_deploy_mode(mode)
        unique_hosts: list[str] = []
        seen: set[str] = set()
        for raw_host in hostnames or []:
            hostname = str(raw_host or "").strip()
            if not hostname or hostname in seen:
                continue
            seen.add(hostname)
            unique_hosts.append(hostname)
        if not unique_hosts:
            raise ValueError("Select at least one VPS.")

        deploy_settings = self.get_vps_deploy_settings()
        base_extra_vars = self._vps_deploy_extra_vars(normalized_command, deploy_settings) or {}
        if extra_vars:
            base_extra_vars.update(extra_vars)

        host_logs: dict[str, dict[str, Any]] = {}
        if normalized_mode == "parallel" or (normalized_mode == "sequential" and len(unique_hosts) == 1):
            for hostname in unique_hosts:
                vps = self._require_vps(hostname)
                self._apply_session_secrets_to_vps(token, vps)
                if _vps_deploy_requires_user_password(normalized_command) and not getattr(vps, "user_pw", None):
                    raise ValueError(f"VPS sudo password missing for {hostname}. Validate it before starting {_vps_deploy_command_text(normalized_command)}.")
                vps.command = normalized_command
                vps.command_text = _vps_deploy_command_text(normalized_command)
                self.vpsmanager.update_vps(vps, debug=debug, extra_vars=base_extra_vars or None)
                task_log_name = vps._task_log_path(vps.command, COMMAND_VPS_UPDATE).name
                host_logs[hostname] = {
                    "command": normalized_command,
                    "command_text": _vps_deploy_command_text(normalized_command),
                    "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "run_id": str(vps.command_run_id or ""),
                    "filename": task_log_name,
                    "file_alias": f"VPSAction:{hostname}:{task_log_name}",
                }

        options: dict[str, Any] = {}
        if normalized_command == COMMAND_VPS_UPDATE:
            reboot_requested = bool(base_extra_vars.get("reboot_requested") or base_extra_vars.get("reboot"))
            options["reboot_requested"] = reboot_requested
        entry = self._record_vps_deploy(
            command=normalized_command,
            mode=normalized_mode,
            hostnames=unique_hosts,
            host_logs=host_logs,
            options=options,
        ) if record_history else {
            "id": "",
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "command": normalized_command,
            "command_text": _vps_deploy_command_text(normalized_command),
            "mode": normalized_mode,
            "options": options,
            "hostnames": list(unique_hosts),
            "services": [],
            "host_logs": host_logs,
            "host_count": len(unique_hosts),
            "service_count": 0,
        }

        if normalized_mode == "sequential" and len(unique_hosts) > 1 and record_history:
            thread = threading.Thread(
                target=self._run_vps_deploy_batch,
                kwargs={
                    "token": token,
                    "command": normalized_command,
                    "mode": normalized_mode,
                    "hostnames": unique_hosts,
                    "debug": debug,
                    "extra_vars": base_extra_vars or None,
                    "entry_id": str(entry.get("id") or ""),
                },
                daemon=True,
                name=f"vps-deploy-{entry.get('id') or int(time.time())}",
            )
            self._deploy_threads[str(entry.get("id") or "")] = thread
            thread.start()

        return {
            "command": normalized_command,
            "command_text": _vps_deploy_command_text(normalized_command),
            "mode": normalized_mode,
            "hostnames": unique_hosts,
            "count": len(unique_hosts),
            "entry": entry,
        }

    def validate_and_stage_vps_deploy_host(
        self,
        token: str,
        *,
        hostnames: list[Any],
        hostname: str,
        password: str,
        command: str,
        mode: str,
        debug: bool = False,
        extra_vars: dict[str, Any] | None = None,
        entry_id: str | None = None,
        accept_unknown_host: bool = False,
    ) -> dict[str, Any]:
        normalized_command = _normalize_vps_deploy_command(command)
        normalized_mode = _normalize_vps_deploy_mode(mode)
        unique_hosts: list[str] = []
        seen: set[str] = set()
        for raw_host in hostnames or []:
            clean_host = str(raw_host or "").strip()
            if not clean_host or clean_host in seen:
                continue
            seen.add(clean_host)
            unique_hosts.append(clean_host)
        clean_hostname = str(hostname or "").strip()
        if not clean_hostname or clean_hostname not in unique_hosts:
            raise ValueError("Selected VPS host is not part of this deploy.")
        if not _vps_deploy_requires_user_password(normalized_command):
            raise ValueError("Password validation staging is only supported for commands that require VPS sudo access.")

        self._validate_vps_user_password(clean_hostname, password, accept_unknown_host=accept_unknown_host)
        self._store_session_secrets(token, clean_hostname, {"user_pw": str(password or "")})

        deploy_settings = self.get_vps_deploy_settings()
        base_extra_vars = self._vps_deploy_extra_vars(normalized_command, deploy_settings) or {}
        if extra_vars:
            base_extra_vars.update(extra_vars)
        options = self._deploy_entry_options(normalized_command, base_extra_vars)

        target_entry_id = str(entry_id or "").strip()
        entry: dict[str, Any]
        with self._deploy_sessions_lock:
            session = self._deploy_sessions.get(target_entry_id) if target_entry_id else None
        if not target_entry_id:
            entry = self._record_vps_deploy(
                command=normalized_command,
                mode=normalized_mode,
                hostnames=unique_hosts,
                host_logs={},
                options=options,
            )
            target_entry_id = str(entry.get("id") or "")
            with self._deploy_sessions_lock:
                self._deploy_sessions[target_entry_id] = {
                    "token": token,
                    "command": normalized_command,
                    "mode": normalized_mode,
                    "debug": bool(debug),
                    "extra_vars": dict(base_extra_vars or {}),
                    "hostnames": list(unique_hosts),
                    "started": set(),
                    "pending": [],
                    "current_host": "",
                    "current_run_id": "",
                    "finalized": False,
                    "wake_event": threading.Event(),
                    "worker": None,
                }
        else:
            if session is None:
                raise ValueError("Deploy session not found.")
            entry = self._get_vps_deploy_entry(target_entry_id)

        started = False
        queued = False
        with self._deploy_sessions_lock:
            session = self._deploy_sessions.get(target_entry_id)
            if not session:
                raise ValueError("Deploy session not found.")
            started_hosts = set(session.get("started") or set())
            pending_hosts = [str(item or "").strip() for item in (session.get("pending") or []) if str(item or "").strip()]
            current_host = str(session.get("current_host") or "")
            if clean_hostname in started_hosts or clean_hostname in pending_hosts or clean_hostname == current_host:
                return {
                    "command": normalized_command,
                    "command_text": _vps_deploy_command_text(normalized_command),
                    "mode": normalized_mode,
                    "hostnames": unique_hosts,
                    "count": len(unique_hosts),
                    "hostname": clean_hostname,
                    "started": False,
                    "queued": clean_hostname in pending_hosts,
                    "entry_id": target_entry_id,
                    "entry": entry,
                }
            start_now = normalized_mode == "parallel" or (not current_host and not pending_hosts)
            if not start_now:
                pending_hosts.append(clean_hostname)
                session["pending"] = pending_hosts
                wake_event = session.get("wake_event")
                if isinstance(wake_event, threading.Event):
                    wake_event.set()
                queued = True

        if start_now:
            host_log = self._start_vps_deploy_host(
                token,
                hostname=clean_hostname,
                command=normalized_command,
                debug=debug,
                extra_vars=base_extra_vars or None,
            )
            entry = self._update_vps_deploy_entry(target_entry_id, host_logs={clean_hostname: host_log})
            started = True
            with self._deploy_sessions_lock:
                session = self._deploy_sessions.get(target_entry_id)
                if session:
                    started_hosts = set(session.get("started") or set())
                    started_hosts.add(clean_hostname)
                    session["started"] = started_hosts
                    if normalized_mode == "sequential":
                        session["current_host"] = clean_hostname
                        session["current_run_id"] = str(host_log.get("run_id") or "")
            if normalized_mode == "sequential":
                self._ensure_vps_deploy_session_worker(target_entry_id)
        else:
            entry = self._get_vps_deploy_entry(target_entry_id)

        return {
            "command": normalized_command,
            "command_text": _vps_deploy_command_text(normalized_command),
            "mode": normalized_mode,
            "hostnames": unique_hosts,
            "count": len(unique_hosts),
            "hostname": clean_hostname,
            "started": started,
            "queued": queued,
            "entry_id": target_entry_id,
            "entry": entry,
        }

    def finalize_vps_deploy_session(self, entry_id: str) -> dict[str, Any]:
        target_entry_id = str(entry_id or "").strip()
        if not target_entry_id:
            raise ValueError("Deploy session id is required.")
        with self._deploy_sessions_lock:
            session = self._deploy_sessions.get(target_entry_id)
            if not session:
                return {"entry_id": target_entry_id, "finalized": True}
            if str(session.get("mode") or "") == "parallel":
                self._deploy_sessions.pop(target_entry_id, None)
            else:
                session["finalized"] = True
                wake_event = session.get("wake_event")
                if isinstance(wake_event, threading.Event):
                    wake_event.set()
        return {"entry_id": target_entry_id, "finalized": True}

    def deploy_vps_logging(self, token: str, hostnames: list[Any], *, debug: bool = False) -> dict[str, Any]:
        settings = self.get_vps_deploy_settings()
        return self.run_vps_deploy(
            token,
            hostnames,
            command=COMMAND_VPS_DEPLOY_LOGGING,
            mode=str(settings.get("mode") or VPS_DEPLOY_DEFAULT_MODE),
            debug=debug,
        )

    def _build_monitor_payload(self, host_state: dict[str, Any], hostname: str | None = None) -> dict[str, Any]:
        if hostname is None:
            return self._empty_monitor_payload()
        return self._build_remote_monitor_payload(hostname, host_state)

    def _build_remote_monitor_payload(self, hostname: str,
                                      host_state: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "server": self._build_remote_server_metrics(hostname, host_state),
            "v7": [],
            "v7_running": [],
            "multi": [],
            "single": [],
            "logfiles": [],
        }
        cfg = self.monitor_config
        meta = self._host_meta(host_state)
        for monitor in (host_state or {}).get("instances") or []:
            metrics = monitor.get("m") or []
            swap_value = metrics[9] / 1024 / 1024 if len(metrics) == 10 else 0.0
            start_ts = _safe_int(monitor.get("st"))
            bot_name = str(monitor.get("u") or "")
            pnl_hist_total, pnls_hist_total = self._bot_pnl_total(hostname, bot_name)
            item = {
                "server": hostname,
                "version": meta.get("pb7v", "N/A"),
                "name": bot_name,
                "pb_version": "7",
                "start_time": datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S") if start_ts else "",
                "memory_mb": round(_safe_float(metrics[0]) / 1024 / 1024, 2) if metrics else 0.0,
                "swap_mb": round(swap_value, 2),
                "cpu": round(_safe_float(monitor.get("c")), 2),
                "cpu_60s": round(_safe_float(monitor.get("cpu_60s")), 2),
                "cpu_60s_window": round(_safe_float(monitor.get("cpu_60s_window")), 1),
                "cpu_60s_confirmed": _safe_float(monitor.get("cpu_60s_window")) >= 60,
                "pnls_today": _safe_int(monitor.get("ct")),
                "pnl_today": _safe_float(monitor.get("pt")),
                "pnls_hist_total": pnls_hist_total,
                "pnl_hist_total": pnl_hist_total,
                "errors_today": _safe_int(monitor.get("et")),
                "errors_4w": self._bot_count_total(hostname, bot_name, "errors"),
                "tracebacks_today": _safe_int(monitor.get("tt")),
                "tracebacks_4w": self._bot_count_total(hostname, bot_name, "tracebacks"),
            }
            item["levels"] = {
                "cpu": _metric_level(item["cpu"], cfg.cpu_warning_v7, cfg.cpu_error_v7),
                "memory": _metric_level(item["memory_mb"], cfg.mem_warning_v7, cfg.mem_error_v7),
                "swap": _metric_level(item["swap_mb"], cfg.swap_warning_v7, cfg.swap_error_v7),
                "errors": _metric_level(item["errors_today"], cfg.error_warning_v7, cfg.error_error_v7),
                "tracebacks": _metric_level(item["tracebacks_today"], cfg.traceback_warning_v7, cfg.traceback_error_v7),
            }
            payload["v7"].append(item)
            if item["name"]:
                payload["logfiles"].append(f"run_v7/{item['name']}/passivbot.log")

        existing_v7_names = {item["name"] for item in payload["v7"] if item.get("name")}
        payload["v7_running"] = self._build_running_v7_payload_from_telemetry(host_state, existing_v7_names)
        for item in payload["v7_running"]:
            if item.get("name"):
                payload["logfiles"].append(f"run_v7/{item['name']}/passivbot.log")
        return payload

    def _build_running_v7_payload_from_telemetry(self, host_state: dict[str, Any],
                                                 existing_names: set[str] | None = None) -> list[dict[str, Any]]:
        known_names = existing_names or set()
        items: list[dict[str, Any]] = []
        for instance in (host_state or {}).get("v7_instances") or []:
            if not _truthy(instance.get("running")):
                continue
            name = str(instance.get("name") or "")
            if not name or name in known_names:
                continue
            items.append(
                {
                    "name": name,
                    "version": _safe_int(instance.get("cv")),
                    "enabled_on": str(instance.get("eo") or ""),
                    "activate_ts": "",
                }
            )
        items.sort(key=lambda item: item["name"])
        return items

    def _require_vps(self, hostname: str) -> VPS:
        vps = self.vpsmanager.find_vps_by_hostname(hostname)
        if not vps:
            raise ValueError(f"Unknown VPS: {hostname}")
        return vps

    def load_more_commits(self, repo: str, branch_name: str, limit: int) -> None:
        pbremote = self._ensure_pbremote()
        if repo == "pbgui":
            commits = load_more_pbgui_commits(branch_name, limit=int(limit))
            if commits:
                release_info = dict(self._get_pbgui_release())
                branches = dict(release_info.get("branches") or {})
                branches[branch_name] = commits
                release_info["branches"] = branches
                self._pbgui_release = release_info
        elif repo == "pb7":
            commits = load_more_pb7_commits(branch_name, _configured_pb7dir(), int(limit))
            if commits:
                release_info = dict(self._get_pb7_release())
                branches = dict(release_info.get("branches") or {})
                branches[branch_name] = commits
                release_info["branches"] = branches
                self._pb7_release = release_info
        else:
            raise ValueError(f"Unknown repo: {repo}")

    def load_remote_branches(self, remote_url: str) -> list[str]:
        if not remote_url:
            return []
        return list_remote_git_branches(remote_url)

    def load_remote_branch_commits(self, remote_url: str, branch_name: str, limit: int = 50) -> list[dict[str, Any]]:
        if not remote_url or not branch_name:
            return []
        return list_remote_git_branch_commits(remote_url, branch_name, limit=int(limit))

    def run_master_command(self, *, command: str, command_text: str, debug: bool = False, sudo_pw: str | None = None, extra_vars: dict[str, Any] | None = None) -> None:
        self.vpsmanager.command = command
        self.vpsmanager.command_text = command_text
        self.vpsmanager.update_master(debug=debug, sudo_pw=sudo_pw, extra_vars=extra_vars)

    def run_vps_command(self, *, token: str, hostname: str, command: str, command_text: str, debug: bool = False, extra_vars: dict[str, Any] | None = None) -> None:
        vps = self._require_vps(hostname)
        self._apply_session_secrets_to_vps(token, vps)
        vps.command = command
        vps.command_text = command_text
        self.vpsmanager.update_vps(vps, debug=debug, extra_vars=extra_vars)

    def delete_vps(self, hostname: str) -> None:
        vps = self._require_vps(hostname)
        vps.delete()
        self.vpsmanager.vpss = [item for item in self.vpsmanager.vpss if item.hostname != hostname]
        self._set_vps_monitor_enabled(hostname, enabled=False)
        self._invalidate_bucket_cleanup_indicator()

    def read_vps_settings(self, token: str, hostname: str) -> dict[str, Any]:
        pbremote = self._ensure_pbremote()
        vps = self._require_vps(hostname)
        vps.user_pw = self._require_user_password(token, hostname)
        if not vps.can_login_ssh():
            raise ValueError("Cannot login via SSH. Please check username and password.")
        vps.bucket = pbremote.bucket
        info = vps.fetch_vps_info()
        vps.coinmarketcap_api_key = info["coinmarketcap"]
        vps.swap = info.get("swap", "0") if info.get("swap") in SWAP_OPTIONS else "0"
        vps.firewall, vps.firewall_ssh_ips = vps.fetch_ufw_settings()
        vps.save()
        return self._build_vps_config(token, vps)

    def save_vps(self, token: str, form: dict[str, Any]) -> dict[str, Any]:
        vps, is_new = self._hydrate_vps_from_form(token, form, allow_create=True)
        self._apply_vps_setup_form(token, vps, form)
        vps.save()
        if is_new:
            self.vpsmanager.vpss.append(vps)
            self.vpsmanager.vpss.sort(key=lambda item: item.hostname or "")
        return self._build_vps_config(token, vps)

    def prepare_import(self, hostname: Any) -> dict[str, Any]:
        hostname = str(hostname or "").strip()
        if not hostname:
            raise ValueError("Hostname is required.")
        if hostname in self.vpsmanager.list():
            raise ValueError("Hostname already exists.")
        temp = VPS()
        temp.hostname = hostname
        ip = str(temp.fetch_vps_ip_from_hosts() or "").strip()
        if not ip:
            raise ValueError("Hostname is not available in local /etc/hosts.")
        if not _valid_ipv4(ip):
            raise ValueError("Hostname in local /etc/hosts does not resolve to a valid IPv4 address.")
        return {
            "hostname": hostname,
            "ip": ip,
            "user": getpass.getuser(),
        }

    def save_vps_config(self, token: str, hostname: str, form: dict[str, Any]) -> dict[str, Any]:
        vps = self._require_vps(hostname)
        self._apply_vps_setup_form(token, vps, form)
        vps.save()
        return self._build_vps_config(token, vps)

    def init_vps(self, token: str, form: dict[str, Any], *, debug: bool = False) -> dict[str, Any]:
        vps, is_new = self._hydrate_vps_from_form(token, form, allow_create=True)
        self._apply_vps_setup_form(token, vps, form)
        self._apply_session_secrets_to_vps(token, vps)
        if not vps.has_init_parameters():
            raise ValueError("Init parameters are incomplete.")
        if not vps.has_setup_parameters():
            raise ValueError("Setup parameters are incomplete.")
        self.vpsmanager.init_vps(vps, debug=debug, auto_setup=True)
        if is_new:
            self.vpsmanager.vpss.append(vps)
            self.vpsmanager.vpss.sort(key=lambda item: item.hostname or "")
        return self._build_vps_progress(vps, include_logs=True)

    def _set_vps_monitor_enabled(self, hostname: str, *, enabled: bool) -> None:
        hostname = str(hostname or "").strip()
        if not hostname:
            return
        current = str(load_ini("vps_monitor", "enabled_hosts") or "")
        hosts = {item.strip() for item in current.split(",") if item.strip()}
        if enabled:
            hosts.add(hostname)
        else:
            hosts.discard(hostname)
        save_ini("vps_monitor", "enabled_hosts", ",".join(sorted(hosts)))

    def setup_vps(self, token: str, hostname: str, form: dict[str, Any], *, debug: bool = False) -> dict[str, Any]:
        vps = self._require_vps(hostname)
        self._apply_vps_setup_form(token, vps, form)
        self._apply_session_secrets_to_vps(token, vps)
        if not vps.has_setup_parameters():
            raise ValueError("Setup parameters are incomplete.")
        self.vpsmanager.setup_vps(vps, debug=debug, extra_vars={"vps_logging_services": self.get_vps_logging_config().get("services") or []})
        self._invalidate_bucket_cleanup_indicator()
        return self._build_vps_progress(vps, include_logs=True)

    def fetch_vps_log(self, hostname: str, *, filename: str, size_kb: int, reverse: bool = True, debug: bool = False) -> dict[str, Any]:
        vps = self._require_vps(hostname)
        vps.logfilename = filename
        vps.logsize = int(size_kb)
        vps.command = "vps-fetch-logfile"
        vps.command_text = f"Fetch logfile {filename}"
        self.vpsmanager.fetch_log(vps, debug=debug)
        content = vps.logfile or ""
        if reverse:
            content = "\n".join(content.splitlines()[::-1])
        return {"filename": filename, "size_kb": int(size_kb), "content": content}

    def _hydrate_vps_from_form(self, token: str, form: dict[str, Any], *, allow_create: bool) -> tuple[VPS, bool]:
        hostname = str(form.get("hostname") or "").strip()
        if not hostname:
            raise ValueError("Hostname is required.")
        vps = self.vpsmanager.find_vps_by_hostname(hostname)
        is_new = vps is None
        if is_new:
            if not allow_create:
                raise ValueError(f"Unknown VPS: {hostname}")
            vps = VPS()
            vps.hostname = hostname
        self._apply_vps_full_form(token, vps, form, is_new=is_new)
        return vps, is_new

    def _apply_vps_full_form(self, token: str, vps: VPS, form: dict[str, Any], *, is_new: bool) -> None:
        master_name = str(self._ensure_pbremote().name or "").strip()
        ip = str(form.get("ip") or "").strip()
        if ip and not _valid_ipv4(ip):
            raise ValueError("IP address is not valid.")
        hostname = str(form.get("hostname") or vps.hostname or "").strip()
        if hostname == master_name:
            raise ValueError("Hostname is equal to master, use another hostname.")
        if is_new and hostname in self.vpsmanager.list():
            raise ValueError("Hostname already exists.")
        init_methode = str(form.get("init_methode") or "root").strip()
        if init_methode not in INIT_METHODS:
            raise ValueError("Invalid init method.")

        for field_name in ("root_pw", "user_sudo_pw", "user_pw"):
            value = str(form.get(field_name) or "")
            if value and ("{{" in value or "}}" in value):
                raise ValueError(f"{field_name} contains '{{{{' or '}}}}'.")

        vps.hostname = hostname
        vps.ip = ip or vps.ip
        vps.init_methode = init_methode
        vps.remove_user = _truthy(form.get("remove_user"))
        vps.user = str(form.get("user") or vps.user or "")
        self._store_session_secrets(token, hostname, form)

    def _apply_vps_setup_form(self, token: str, vps: VPS, form: dict[str, Any]) -> None:
        hostname = str(vps.hostname or "")
        self._store_session_secrets(token, hostname, form)
        user_pw = str(form.get("user_pw") or self._session_secret_value(token, hostname, "user_pw") or "")
        if user_pw and ("{{" in user_pw or "}}" in user_pw):
            raise ValueError("user_pw contains '{{' or '}}'.")
        swap = str(form.get("swap") or vps.swap or "0")
        if swap not in SWAP_OPTIONS:
            raise ValueError("Invalid swap size.")
        firewall_ips = str(form.get("firewall_ssh_ips") or "").strip()
        if firewall_ips:
            for ip in [part.strip() for part in firewall_ips.split(",") if part.strip()]:
                if not _valid_ipv4(ip):
                    raise ValueError("IP-Addresses to allow contains an invalid IPv4 address.")
        vps.user_pw = user_pw or None
        vps.swap = swap
        vps.bucket = str(form.get("bucket") or vps.bucket or "")
        vps.coinmarketcap_api_key = str(form.get("coinmarketcap_api_key") or vps.coinmarketcap_api_key or "")
        vps.firewall = _truthy(form.get("firewall"))
        vps.firewall_ssh_port = _safe_int(form.get("firewall_ssh_port"), 22)
        vps.firewall_ssh_ips = firewall_ips
        vps.save()

    def browse_files(self, path: str) -> dict[str, Any]:
        import os
        start = str(Path.home()) if not path else os.path.expanduser(path)
        if not os.path.isdir(start):
            start = os.path.dirname(start) or str(Path.home())
        entries: list[dict[str, str]] = []
        try:
            for name in sorted(os.listdir(start)):
                full = os.path.join(start, name)
                entries.append({"name": name, "type": "dir" if os.path.isdir(full) else "file"})
        except PermissionError:
            pass
        return {"cwd": start, "parent": os.path.dirname(start) or start, "entries": entries}

    def check_vps_ready(self, form: dict[str, Any]) -> dict[str, Any]:
        import socket
        import paramiko

        ip = str(form.get("ip") or "").strip()
        hostname = str(form.get("hostname") or "").strip()
        init_method = str(form.get("init_methode") or "root")

        result = {"hosts_ok": False, "hosts_has_hostname": False, "hosts_current_ip": "", "ssh_ok": False, "ssh_error": ""}

        if not ip or not hostname:
            result["ssh_error"] = "IP and hostname required"
            return result

        try:
            with open("/etc/hosts", "r") as f:
                for line in f:
                    line = line.split("#")[0].strip()
                    if not line:
                        continue
                    parts = line.split()
                    if len(parts) >= 2:
                        line_ip = parts[0]
                        hosts = parts[1:]
                        if hostname in hosts:
                            result["hosts_has_hostname"] = True
                            result["hosts_current_ip"] = line_ip
                            if line_ip == ip:
                                result["hosts_ok"] = True
                                break
        except Exception:
            pass

        if not result["hosts_ok"]:
            if result["hosts_has_hostname"]:
                result["ssh_error"] = f"Hostname found with IP {result['hosts_current_ip']} instead of {ip}"
            else:
                result["ssh_error"] = "IP/hostname not found in local /etc/hosts"
            return result

        ssh_user = None
        ssh_pw = None
        ssh_key = None

        if init_method == "root":
            ssh_user = "root"
            ssh_pw = str(form.get("initial_root_pw") or "").strip() or None
        elif init_method == "password":
            ssh_user = str(form.get("user_sudo") or "").strip() or None
            ssh_pw = str(form.get("user_sudo_pw") or "").strip() or None
        elif init_method == "private_key":
            ssh_user = str(form.get("private_key_user") or "").strip() or None
            ssh_key = str(form.get("private_key_file") or "").strip() or None

        if not ssh_user:
            result["ssh_error"] = "SSH user not configured"
            return result
        if init_method == "private_key" and not ssh_key:
            result["ssh_error"] = "Private key file not selected"
            return result
        if init_method != "private_key" and not ssh_pw:
            result["ssh_error"] = "SSH password not entered"
            return result

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            kwargs = {"hostname": ip, "username": ssh_user, "timeout": 8, "banner_timeout": 8, "auth_timeout": 8}
            if init_method == "private_key":
                kwargs["key_filename"] = ssh_key
            else:
                kwargs["password"] = ssh_pw
            ssh.connect(**kwargs)
            result["ssh_ok"] = True
        except paramiko.AuthenticationException:
            result["ssh_error"] = "Authentication failed"
        except (paramiko.SSHException, socket.timeout, OSError) as exc:
            result["ssh_error"] = str(exc) or "SSH connection failed"
        finally:
            try:
                ssh.close()
            except Exception:
                pass

        return result

    def write_hosts_entry(self, ip: str, hostname: str, sudo_pw: str) -> dict[str, Any]:
        import subprocess
        ip = str(ip or "").strip()
        hostname = str(hostname or "").strip()
        pw = str(sudo_pw or "").strip()
        if not ip or not hostname:
            return {"ok": False, "error": "IP and hostname required"}
        if not pw:
            return {"ok": False, "error": "Sudo password required"}

        try:
            with open("/etc/hosts", "r") as f:
                raw_lines = f.readlines()
        except Exception as exc:
            return {"ok": False, "error": f"Cannot read /etc/hosts: {exc}"}

        new_lines: list[str] = []
        ip_line_index = -1
        for i, line in enumerate(raw_lines):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                new_lines.append(line)
                continue
            parts = stripped.split()
            if len(parts) < 2:
                new_lines.append(line)
                continue
            line_ip = parts[0]
            hosts = parts[1:]
            if hostname in hosts:
                hosts = [h for h in hosts if h != hostname]
            if not hosts:
                continue
            new_line = f"{line_ip}\t{' '.join(hosts)}\n"
            new_lines.append(new_line)
            if line_ip == ip:
                ip_line_index = len(new_lines) - 1

        if ip_line_index >= 0:
            existing = new_lines[ip_line_index].rstrip("\n").split()
            if hostname not in existing:
                existing.append(hostname)
                new_lines[ip_line_index] = f"{'\t'.join(existing)}\n"
        else:
            new_lines.append(f"{ip}\t{hostname}\n")

        content = "".join(new_lines)
        try:
            proc = subprocess.Popen(
                ["sudo", "-S", "tee", "/etc/hosts"],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            _, err = proc.communicate(input=pw + "\n" + content, timeout=15)
            if proc.returncode != 0:
                return {"ok": False, "error": err.strip() or "sudo tee failed"}
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": "sudo tee timed out"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def validate_local_sudo_password(self, sudo_pw: str) -> dict[str, Any]:
        import subprocess

        pw = str(sudo_pw or "").strip()
        if not pw:
            return {"ok": False, "error": "Sudo password required"}

        try:
            proc = subprocess.Popen(
                ["sudo", "-S", "-p", "", "true"],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            _, err = proc.communicate(input=pw + "\n", timeout=15)
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": "Local sudo validation timed out"}
        except Exception as exc:
            return {"ok": False, "error": f"Failed to validate local sudo access: {exc}"}

        stderr_text = str(err or "").strip()
        stderr_lower = stderr_text.lower()
        if proc.returncode == 0:
            return {"ok": True}
        if "no new privileges" in stderr_lower or "keine neuen privilegien" in stderr_lower:
            return {
                "ok": False,
                "blocked": True,
                "error": "Local sudo is blocked by the current runtime (`NoNewPrivs`).",
            }
        return {
            "ok": False,
            "error": stderr_text or "Incorrect sudo password or local sudo unavailable",
        }

    def check_bucket(self, bucket_name: str) -> dict[str, Any]:
        pbremote = self._ensure_pbremote()
        bucket = str(bucket_name or "").strip()
        if not bucket:
            return {"ok": False, "error": "Bucket name is empty"}
        rclone_installed = getattr(pbremote, "rclone_installed", False)
        if not rclone_installed:
            return {"ok": False, "error": "rclone not installed locally"}
        old_bucket = getattr(pbremote, "bucket", "")
        try:
            pbremote.bucket = bucket if bucket.endswith(":") else f"{bucket}:"
            ok, output = pbremote.test_bucket()
            return {"ok": bool(ok), "error": "" if ok else str(output or "Bucket check failed")}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        finally:
            pbremote.bucket = old_bucket

    def check_cmc_api_key(self, api_key: str) -> dict[str, Any]:
        key = str(api_key or "").strip()
        if not key:
            return {"ok": False, "error": "API key is empty"}
        coindata = self._ensure_coindata()
        old_key = coindata.api_key
        try:
            coindata.api_key = key
            ok = coindata.fetch_api_status()
            if not ok:
                return {"ok": False, "error": getattr(coindata, "api_error", "CoinMarketCap API key is invalid")}
            return {
                "ok": True,
                "error": "",
                "credit_limit_monthly": getattr(coindata, "credit_limit_monthly", None),
                "credits_used_day": getattr(coindata, "credits_used_day", None),
                "credits_used_month": getattr(coindata, "credits_used_month", None),
                "credits_left": getattr(coindata, "credits_left", None),
                "credit_limit_monthly_reset_timestamp": getattr(coindata, "credit_limit_monthly_reset_timestamp", None),
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        finally:
            coindata.api_key = old_key
