from __future__ import annotations

import asyncio
import base64
import configparser
import getpass
import hashlib
import ipaddress
import json
import os
import psutil
import re
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any, Callable
import yaml

from api.vps import get_monitor, get_monitor_state_snapshot, get_metric_history_snapshot
from logging_helpers import human_log as _log
from master.async_monitor import INSTANCE_COLLECT_SCRIPT, METRICS_STREAM_STALE_SECONDS, MONITOR_CACHE_VERSION
from master.cluster_state import ClusterStateError, default_cluster_root, normalize_node_sync_mode, read_local_identity, rebuild_materialized_state
from MonitorConfig import MonitorConfig
from PBCoinData import CoinData
from pb7_release import build_local_pb7_release_info, get_current_pb7_status, load_more_pb7_commits
from pbgui_release import build_local_pbgui_release_info, load_more_pbgui_commits
from pbgui_purefunc import get_git_branch_remote, get_git_branch_remotes, get_git_remote_url, list_git_remotes, list_remote_git_branch_commits, list_remote_git_branches, load_ini, load_ini_section, pb7dir as configured_pb7dir, save_ini, save_ini_section
from vps_manager_core import PBGDIR, VPS, VPSManager, _install_dir_from_remote_pbgui_dir, _register_vps_cluster_node, strip_ansi

SERVICE = "VPSManagerApi"

PB7_UPSTREAM_REMOTE_NAME = "origin"
PB7_UPSTREAM_REMOTE_URL = "https://github.com/enarjord/passivbot.git"
SWAP_OPTIONS = ["0", "1G", "1.5G", "2G", "2.5G", "3G", "4G", "5G", "6G", "8G"]
INIT_METHODS = ["root", "password", "private_key"]
SESSION_SECRET_TTL_SECONDS = 15 * 60
CLUSTER_IMPORT_JOB_TTL_SECONDS = 30 * 60
IMPORT_KEY_INSTALL_WARNING = "SSH key login is not available yet; saving the import will attempt to install this master's SSH public key for live monitoring."
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
VPS_LOGGING_SERVICES = ("PBRun", "PBCluster", "PBCoinData", "sync", "vps_cleanup", "tradfi_sync")
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
COMMAND_VPS_APPLY_CONFIG = "vps-apply-config"
COMMAND_VPS_MIGRATE_SYSTEMD = "vps-migrate-systemd"
OPTIONAL_VPS_CONFIG_FIELDS = ("coinmarketcap_api_key",)
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
SAFE_VPS_INSTALL_PATH_RE = re.compile(r"^[A-Za-z0-9._~/-]+$")
VPS_SYSTEMD_MIGRATION_SERVICES = ("pbcluster", "pbrun", "pbdata", "pbcoindata")
VPS_SYSTEMD_MIGRATION_UNITS = tuple(f"pbgui-{service}.service" for service in VPS_SYSTEMD_MIGRATION_SERVICES)
VPS_SYSTEMD_MIGRATION_STATUS_TTL_SECONDS = 90
_PLAYBOOK_TASK_CACHE: dict[str, tuple[str, ...]] = {}


class UnknownHostKeyError(ValueError):
    def __init__(self, *, hostname: str, ssh_host: str, ip: str) -> None:
        self.hostname = str(hostname or "")
        self.ssh_host = str(ssh_host or "")
        self.ip = str(ip or "")
        target = self.ssh_host or self.ip or self.hostname
        super().__init__(f"Server '{target}' not found in known_hosts")


def _ssh_fingerprint_sha256(key: Any) -> str:
    digest = hashlib.sha256(key.asbytes()).digest()
    encoded = base64.b64encode(digest).decode("ascii").rstrip("=")
    return f"SHA256:{encoded}"


def _normalize_ssh_fingerprint(value: str) -> str:
    text = str(value or "").strip()
    return text[7:] if text.startswith("SHA256:") else text


def _ssh_fingerprints_match(expected: str, actual: str) -> bool:
    return bool(expected) and _normalize_ssh_fingerprint(expected) == _normalize_ssh_fingerprint(actual)


def _user_known_hosts_path() -> Path:
    return Path.home() / ".ssh" / "known_hosts"


def _load_known_hosts() -> Any:
    import paramiko

    host_keys = paramiko.HostKeys()
    for path in (Path("/etc/ssh/ssh_known_hosts"), _user_known_hosts_path()):
        try:
            if path.exists():
                host_keys.load(str(path))
        except Exception:
            continue
    return host_keys


def _host_key_names(host: str, port: int = 22) -> list[str]:
    clean_host = str(host or "").strip()
    if not clean_host:
        return []
    names = [clean_host]
    if int(port) != 22:
        names.insert(0, f"[{clean_host}]:{int(port)}")
    return names


def _known_host_key_status(host: str, port: int, key: Any) -> str:
    host_keys = _load_known_hosts()
    matched = False
    mismatch = False
    for name in _host_key_names(host, port):
        entries = host_keys.lookup(name)
        if not entries:
            continue
        known_key = entries.get(key.get_name())
        if known_key is None:
            continue
        if known_key.asbytes() == key.asbytes():
            matched = True
        else:
            mismatch = True
    if mismatch:
        return "mismatch"
    return "known" if matched else "unknown"


def _fetch_remote_host_key(host: str, port: int = 22, timeout: int = 10) -> Any:
    import paramiko

    sock = socket.create_connection((host, int(port)), timeout=timeout)
    transport = paramiko.Transport(sock)
    try:
        transport.start_client(timeout=timeout)
        return transport.get_remote_server_key()
    finally:
        transport.close()


def _remember_known_host_key(host: str, port: int, key: Any) -> None:
    import paramiko

    names = _host_key_names(host, port)
    if not names:
        return
    ssh_dir = Path.home() / ".ssh"
    ssh_dir.mkdir(mode=0o700, exist_ok=True)
    try:
        ssh_dir.chmod(0o700)
    except OSError:
        pass
    path = _user_known_hosts_path()
    host_keys = paramiko.HostKeys()
    if path.exists():
        host_keys.load(str(path))
    host_keys.add(names[0], key.get_name(), key)
    host_keys.save(str(path))
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _validate_import_hostname(value: Any) -> str:
    hostname = str(value or "").strip()
    if not hostname:
        raise ValueError("Hostname is required.")
    if hostname in {".", ".."} or any(ch in hostname for ch in ("/", "\\", "\x00")):
        raise ValueError("Hostname contains invalid characters.")
    return hostname


def _hosts_entry_lookup(hostname: str) -> dict[str, Any]:
    result = {"hostname": str(hostname or ""), "found": False, "ip": ""}
    hosts = Path("/etc/hosts")
    if not hosts.exists():
        return result
    try:
        with hosts.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.split("#", 1)[0].strip()
                if not line:
                    continue
                parts = line.split()
                if len(parts) < 2:
                    continue
                line_ip = parts[0]
                names = parts[1:]
                if hostname not in names:
                    continue
                result["found"] = True
                result["ip"] = line_ip
                if _valid_ipv4(line_ip):
                    return result
    except Exception:
        return result
    return result


def _hosts_entry_status(hostname: str, ip: str) -> dict[str, Any]:
    result = {"ok": False, "has_hostname": False, "current_ip": ""}
    lookup = _hosts_entry_lookup(hostname)
    if lookup.get("found"):
        result["has_hostname"] = True
        result["current_ip"] = str(lookup.get("ip") or "")
        result["ok"] = result["current_ip"] == str(ip or "").strip()
    return result


def _sftp_path_exists(sftp: Any, path: str) -> bool:
    try:
        sftp.stat(path)
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return False


def _parse_ufw_status(output: str) -> tuple[bool, str]:
    allowed_ips: list[str] = []
    fw_enabled = bool(re.search(r"^Status:\s+active\b", str(output or ""), re.IGNORECASE | re.MULTILINE))
    pattern = re.compile(r"^(?:22|ssh)/tcp\s+ALLOW\s+(.+)$", re.IGNORECASE)
    for raw_line in str(output or "").splitlines():
        line = raw_line.strip()
        match = pattern.search(line)
        if not match:
            continue
        value = match.group(1).strip().split()[0]
        if value.lower() in {"anywhere", "anywhere(v6)", "anywhere (v6)"}:
            value = ""
        if value and _valid_ipv4(value) and value not in allowed_ips:
            allowed_ips.append(value)
    return fw_enabled, ",".join(allowed_ips)


def _parse_ufw_numbered_status(output: str) -> dict[str, Any]:
    text = str(output or "")
    enabled = bool(re.search(r"^Status:\s+active\b", text, re.IGNORECASE | re.MULTILINE))
    rules: list[dict[str, Any]] = []
    action_re = re.compile(r"\s{2,}(ALLOW(?:\s+IN)?|DENY(?:\s+IN)?|REJECT(?:\s+IN)?|LIMIT(?:\s+IN)?)\s{2,}", re.IGNORECASE)
    for raw_line in text.splitlines():
        line = raw_line.strip()
        match = re.match(r"^\[\s*(\d+)\]\s+(.+)$", line)
        if not match:
            continue
        number = int(match.group(1))
        body = match.group(2).strip()
        comment = ""
        if " #" in body:
            body, comment = body.split(" #", 1)
            comment = comment.strip()
        action_match = action_re.search(body)
        if not action_match:
            continue
        to_value = body[: action_match.start()].strip()
        action = " ".join(action_match.group(1).upper().split())
        from_value = body[action_match.end() :].strip()
        rules.append({"number": number, "to": to_value, "action": action, "from": from_value, "comment": comment})
    fingerprint_src = json.dumps({"enabled": enabled, "rules": rules}, sort_keys=True)
    return {"enabled": enabled, "rules": rules, "fingerprint": hashlib.sha256(fingerprint_src.encode("utf-8")).hexdigest()}


def _ufw_rule_port_proto(rule: dict[str, Any]) -> tuple[int | None, str]:
    value = str(rule.get("to") or "").strip().lower()
    value = re.sub(r"\s*\(v6\)\s*$", "", value).strip()
    match = re.search(r"\b(\d{1,5})(?:/(tcp|udp))?\b", value)
    if not match:
        return None, ""
    port = int(match.group(1))
    proto = str(match.group(2) or "tcp").lower()
    return port, proto


def _ufw_from_allows(from_value: str, client_ip: str) -> bool:
    source = str(from_value or "").strip()
    ip_text = str(client_ip or "").strip()
    if not ip_text:
        return False
    lowered = source.lower()
    if lowered in {"anywhere", "anywhere (v4)", "0.0.0.0/0"}:
        return True
    if "(v6)" in lowered or lowered in {"anywhere (v6)", "::/0"}:
        return False
    source = source.split()[0]
    try:
        ip_obj = ipaddress.ip_address(ip_text)
        if "/" in source:
            return ip_obj in ipaddress.ip_network(source, strict=False)
        return ip_obj == ipaddress.ip_address(source)
    except ValueError:
        return False


def _ufw_has_allow_rule(rules: list[dict[str, Any]], port: int, proto: str, client_ip: str | None = None) -> bool:
    proto = str(proto or "tcp").lower()
    for rule in rules:
        action = str(rule.get("action") or "").upper()
        if not action.startswith("ALLOW"):
            continue
        rule_port, rule_proto = _ufw_rule_port_proto(rule)
        if rule_port != int(port) or rule_proto != proto:
            continue
        if client_ip is None or _ufw_from_allows(str(rule.get("from") or ""), client_ip):
            return True
    return False


def _normalize_ufw_rule(raw: dict[str, Any]) -> dict[str, str]:
    port_raw = str(raw.get("port") or "").strip()
    if not port_raw.isdigit() or not 1 <= int(port_raw) <= 65535:
        raise ValueError("UFW rule port must be between 1 and 65535.")
    proto = str(raw.get("proto") or "tcp").strip().lower()
    if proto not in {"tcp", "udp"}:
        raise ValueError("UFW rule protocol must be tcp or udp.")
    from_value = str(raw.get("from") or "Anywhere").strip() or "Anywhere"
    if from_value.lower() not in {"anywhere", "anywhere (v4)", "0.0.0.0/0"}:
        try:
            if "/" in from_value:
                ipaddress.ip_network(from_value, strict=False)
            else:
                ipaddress.ip_address(from_value)
        except ValueError as exc:
            raise ValueError(f"Invalid UFW source: {from_value}") from exc
    comment = re.sub(r"[\r\n\x00]+", " ", str(raw.get("comment") or "")).strip()[:80]
    return {"port": str(int(port_raw)), "proto": proto, "from": from_value, "comment": comment}


def _ssh_client_ip_from_connection(value: str) -> str:
    parts = str(value or "").strip().split()
    return parts[0] if parts else ""


def _is_vpn_management_ip(client_ip: str) -> bool:
    try:
        ip_obj = ipaddress.ip_address(str(client_ip or "").strip())
    except ValueError:
        return False
    return ip_obj.version == 4 and ip_obj in ipaddress.ip_network("10.8.0.0/16")


def _simulate_ufw_changes(
    current_rules: list[dict[str, Any]],
    delete_numbers: list[int],
    add_rules: list[dict[str, str]],
    enabled: bool,
    client_ip: str = "",
) -> dict[str, Any]:
    delete_set = {int(number) for number in delete_numbers}
    existing_numbers = {int(rule.get("number") or 0) for rule in current_rules}
    missing = sorted(number for number in delete_set if number not in existing_numbers)
    if missing:
        return {"ok": False, "blocking": [f"Selected UFW rule number(s) no longer exist: {', '.join(map(str, missing))}. Refresh UFW first."], "warnings": [], "rules": []}
    simulated = [dict(rule) for rule in current_rules if int(rule.get("number") or 0) not in delete_set]
    next_number = max(existing_numbers or {0}) + 1
    for rule in add_rules:
        from_value = str(rule.get("from") or "Anywhere")
        simulated.append({
            "number": next_number,
            "to": f"{rule['port']}/{rule['proto']}",
            "action": "ALLOW IN",
            "from": from_value,
            "comment": str(rule.get("comment") or ""),
        })
        next_number += 1
    blocking: list[str] = []
    warnings: list[str] = []
    if enabled:
        if client_ip and not _ufw_has_allow_rule(simulated, 22, "tcp", client_ip):
            blocking.append(f"SSH would no longer allow the current management IP {client_ip}.")
        elif not _ufw_has_allow_rule(simulated, 22, "tcp"):
            blocking.append("No SSH allow rule for 22/tcp would remain while UFW is enabled.")
        if _is_vpn_management_ip(client_ip) and not _ufw_has_allow_rule(simulated, 1194, "udp"):
            blocking.append("Current management connection appears to use VPN, but no 1194/udp allow rule would remain.")
        if not _ufw_has_allow_rule(simulated, 8000, "tcp"):
            warnings.append("No PBGui 8000/tcp allow rule would remain.")
        if not _ufw_has_allow_rule(simulated, 1194, "udp"):
            warnings.append("No OpenVPN 1194/udp allow rule would remain.")
    return {"ok": not blocking, "blocking": blocking, "warnings": warnings, "rules": simulated}


def _parse_import_systemd_units(output: str) -> list[dict[str, str]]:
    units: list[dict[str, str]] = []
    for raw_line in str(output or "").splitlines():
        parts = raw_line.split("\t")
        if len(parts) < 4:
            continue
        units.append({"unit": parts[0], "exists": parts[1], "enabled": parts[2], "active": parts[3]})
    return units


def _import_process_line_is_legacy(line: str, pbgui_dir: str) -> bool:
    """Return whether an import probe process row is an unmanaged legacy process."""
    text = str(line or "").strip()
    if not text or str(pbgui_dir or "") not in text:
        return False
    parts = text.split("\t", 3)
    if len(parts) >= 4:
        return parts[2] != "systemd"
    return True


def _read_existing_import_public_key() -> tuple[Path, Path, str] | None:
    """Return an existing default SSH public/private key pair for import monitoring."""
    ssh_dir = Path.home() / ".ssh"
    for public_key_path, private_key_path in (
        (ssh_dir / "id_ed25519.pub", ssh_dir / "id_ed25519"),
        (ssh_dir / "id_rsa.pub", ssh_dir / "id_rsa"),
    ):
        if not public_key_path.exists() or not private_key_path.exists():
            continue
        for line in public_key_path.read_text(encoding="utf-8").splitlines():
            key = line.strip()
            if key:
                return public_key_path, private_key_path, key
    return None


def _ensure_import_public_key() -> tuple[Path, str]:
    """Return a default local public key, generating one when none exists."""
    existing = _read_existing_import_public_key()
    if existing:
        public_key_path, _private_key_path, public_key = existing
        return public_key_path, public_key

    ssh_keygen = shutil.which("ssh-keygen")
    if not ssh_keygen:
        raise RuntimeError("ssh-keygen is required to create an SSH key for VPS monitoring.")

    ssh_dir = Path.home() / ".ssh"
    ssh_dir.mkdir(mode=0o700, exist_ok=True)
    private_key_path = ssh_dir / "id_ed25519"
    public_key_path = ssh_dir / "id_ed25519.pub"
    if private_key_path.exists() and not public_key_path.exists():
        proc = subprocess.run(
            [ssh_keygen, "-y", "-f", str(private_key_path)],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
        public_key = str(proc.stdout or "").strip()
        if proc.returncode != 0 or not public_key:
            output = ((proc.stderr or "") + (proc.stdout or "")).strip()
            raise RuntimeError(output or f"Could not derive public key from {private_key_path}.")
        tmp_path = public_key_path.with_suffix(public_key_path.suffix + ".tmp")
        tmp_path.write_text(public_key + "\n", encoding="utf-8")
        os.replace(tmp_path, public_key_path)
        return public_key_path, public_key

    if private_key_path.exists() or public_key_path.exists():
        raise RuntimeError(f"Incomplete SSH key pair at {private_key_path}; expected both private and public key files.")

    subprocess.run(
        [ssh_keygen, "-t", "ed25519", "-f", str(private_key_path), "-N", "", "-C", "pbgui-vps-monitor"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    public_key = public_key_path.read_text(encoding="utf-8").strip()
    if not public_key:
        raise RuntimeError(f"Generated SSH public key is empty: {public_key_path}")
    return public_key_path, public_key


def _set_import_key_check(probe: dict[str, Any], ok: bool, detail: str) -> None:
    """Replace the import probe monitoring-key check with the latest result."""
    checks = probe.get("checks") if isinstance(probe, dict) else None
    if not isinstance(checks, list):
        return
    for check in checks:
        if isinstance(check, dict) and check.get("label") == "SSH key login for monitoring":
            check["ok"] = bool(ok)
            check["detail"] = str(detail or "")
            return
    checks.append({"label": "SSH key login for monitoring", "ok": bool(ok), "detail": str(detail or "")})


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


def _normalize_vps_install_dir(value: Any, vps_user: str | None) -> str:
    raw = str(value or "").strip().rstrip("/")
    if not raw:
        return ""
    if any(ch in raw for ch in ("\x00", "\n", "\r")) or "{{" in raw or "}}" in raw:
        raise ValueError("Install path contains invalid characters.")
    user = str(vps_user or "").strip()
    home = f"/home/{user}" if user else str(Path.home())
    if raw.startswith("~/"):
        raw = f"{home}/{raw[2:]}"
    elif not raw.startswith("/"):
        raise ValueError("Install path must be absolute or start with '~/'.")
    if not SAFE_VPS_INSTALL_PATH_RE.fullmatch(raw):
        raise ValueError("Install path may only contain letters, numbers, '/', '.', '_', '-' and '~'.")
    if "." in raw.split("/") or ".." in raw.split("/"):
        raise ValueError("Install path cannot contain '.' or '..' path segments.")
    path = PurePosixPath(raw)
    normalized = str(path)
    if normalized == "/":
        raise ValueError("Install path cannot be '/'.")
    return normalized


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


def _configured_optional_secret(value: Any) -> bool:
    normalized = str(value or "").strip()
    if not normalized:
        return False
    lowered = normalized.lower()
    if lowered in {"none", "null", "false", "<api_key>"}:
        return False
    return not (normalized.startswith("<") and normalized.endswith(">"))


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


def _local_master_name() -> str:
    return str(load_ini("main", "pbname") or socket.gethostname() or "local").strip() or "local"


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


def _read_master_update_log_tail(vpsmanager: VPSManager) -> str:
    command = str(getattr(vpsmanager, "command", "") or "").strip()
    log_text = _read_text_tail(vpsmanager._task_log_path(command, "master-update-pb"))
    if log_text:
        return strip_ansi(log_text)
    return strip_ansi(str(getattr(vpsmanager, "update_log", "") or ""))


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
        {"key": "restart_core", "label": "Restart PBRun"},
        {"key": "restart_coindata", "label": "Restart PBCoinData"},
    ]

    current_key = ""
    if "write vps logging section values" in step:
        current_key = "config"
    elif "force single-file capped logging on vps" in step:
        current_key = "cap"
    elif "restart pbrun after logging deploy" in step:
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
        self._master_server_cpu_history: list[tuple[float, float, float]] = []
        self._vps_coindata_status_cache: dict[str, bool] = {}
        self._vps_ssh_ok_cache: dict[str, bool] = {}
        self._vps_systemd_migration_status_cache: dict[str, dict[str, Any]] = {}
        self._session_secrets: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
        self._deploy_threads: dict[str, threading.Thread] = {}
        self._deploy_sessions: dict[str, dict[str, Any]] = {}
        self._deploy_sessions_lock = threading.Lock()
        self._cluster_import_jobs: dict[str, dict[str, Any]] = {}
        self._cluster_import_jobs_lock = threading.Lock()
        self._host_task_start_locks: dict[str, threading.Lock] = {}
        self._host_task_start_locks_lock = threading.Lock()
        self._deploy_history_lock = threading.Lock()
        self._deploy_progress_cache_lock = threading.Lock()
        self._deploy_progress_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._recover_interrupted_vps_runs()

    def _is_vps_playbook_process_running(self, vps: VPS) -> bool:
        if not self._should_treat_vps_process_as_active(vps):
            return False
        return self._vps_playbook_process_exists(vps)

    def _vps_playbook_process_exists(self, vps: VPS) -> bool:
        host_path = getattr(vps, "path", None)
        host_tmp_dir = str((Path(host_path) / "tmp").resolve()) if host_path else ""
        inventory_path = str((Path(host_tmp_dir) / "inventory" / "hosts").resolve()) if host_tmp_dir else ""
        if not host_tmp_dir:
            return False
        playbook_path = str((Path(PBGDIR) / f"{str(getattr(vps, 'command', '') or '').strip()}.yml").resolve())
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

    def _host_task_start_lock(self, hostname: str) -> threading.Lock:
        clean_hostname = str(hostname or "").strip()
        if not hasattr(self, "_host_task_start_locks"):
            self._host_task_start_locks = {}
        if not hasattr(self, "_host_task_start_locks_lock"):
            self._host_task_start_locks_lock = threading.Lock()
        with self._host_task_start_locks_lock:
            lock = self._host_task_start_locks.get(clean_hostname)
            if lock is None:
                lock = threading.Lock()
                self._host_task_start_locks[clean_hostname] = lock
            return lock

    def _raise_if_vps_task_active(self, vps: VPS, command_text: str) -> None:
        if self._should_treat_vps_process_as_active(vps) or self._vps_playbook_process_exists(vps):
            label = str(command_text or "this action").strip() or "this action"
            raise ValueError(f"{vps.hostname} already has an active VPS task. Wait for it to finish before starting {label}.")

    def _is_vps_task_active_error(self, exc: Exception) -> bool:
        return "already has an active VPS task" in str(exc)

    def _skipped_vps_deploy_host_log(self, hostname: str, command: str, reason: str) -> dict[str, Any]:
        normalized_command = _normalize_vps_deploy_command(command)
        return {
            "command": normalized_command,
            "command_text": _vps_deploy_command_text(normalized_command),
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "run_id": "",
            "filename": "",
            "file_alias": "",
            "status": "skipped",
            "reason": str(reason or "Already running on this host."),
        }

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
        log_text = _read_master_update_log_tail(self.vpsmanager)
        command = str(getattr(self.vpsmanager, "command", "") or "").strip()
        if not log_text:
            return True
        parsed_progress = _parse_ansible_task_progress("localhost", log_text, command=command)
        return self._status_from_task_progress(parsed_progress) not in {"successful", "failed", "unreachable"}

    def _recover_completed_master_run(self) -> str:
        status = str(getattr(self.vpsmanager, "update_status", "") or "").strip()
        if not _status_running(status):
            return status
        log_text = _read_master_update_log_tail(self.vpsmanager)
        if not log_text:
            return status
        command = str(getattr(self.vpsmanager, "command", "") or "").strip()
        parsed_progress = _parse_ansible_task_progress("localhost", log_text, command=command)
        parsed_status = self._status_from_task_progress(parsed_progress)
        if parsed_status not in {"successful", "failed", "unreachable"}:
            return status
        final_status = "failed" if parsed_status == "unreachable" else parsed_status
        self.vpsmanager.update_status = final_status
        if not str(getattr(self.vpsmanager, "last_update", "") or "").strip():
            self.vpsmanager.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.vpsmanager.save_master()
        _log(SERVICE, f"recovered completed master task state from log: {final_status}", level="INFO")
        return final_status

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
        self._recover_completed_master_run()
        if self._should_treat_master_process_as_active() or self._is_master_playbook_process_running():
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
        master_status = self._recover_completed_master_run()
        if self._should_treat_master_process_as_active() or self._is_master_playbook_process_running():
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

    def _cluster_nodes_for_vps_import(self) -> tuple[list[dict[str, Any]], str]:
        """Return materialized Cluster nodes and the local node id for VPS import."""

        root = default_cluster_root(Path(PBGDIR))
        try:
            identity = read_local_identity(root)
            materialized = rebuild_materialized_state(root, write=False)
        except ClusterStateError as exc:
            raise ValueError(f"Cluster state is not ready: {exc}") from exc
        cluster_nodes = materialized.get("cluster_nodes") if isinstance(materialized, dict) else {}
        nodes = cluster_nodes.get("nodes") if isinstance(cluster_nodes, dict) else {}
        if not isinstance(nodes, dict):
            return [], str(identity.get("node_id") or "")
        result = [dict(node) for node in nodes.values() if isinstance(node, dict)]
        result.sort(key=lambda item: str(item.get("pbname") or item.get("hostname") or item.get("node_id") or ""))
        return result, str(identity.get("node_id") or "")

    def _cluster_node_vps_import_item(
        self,
        node: dict[str, Any],
        local_node_id: str,
        existing: dict[str, VPS],
        monitor_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build one preview row for importing Cluster SSH metadata into VPS Manager."""

        node_id = str(node.get("node_id") or "").strip()
        hostname = str(node.get("pbname") or node.get("hostname") or "").strip()
        role = str(node.get("role") or "vps").strip() or "vps"
        ssh_host = str(node.get("ssh_host") or "").strip()
        ssh_user = str(node.get("ssh_user") or "").strip()
        ssh_port = _safe_int(node.get("ssh_port"), 22) or 22
        remote_pbgui_dir = str(node.get("remote_pbgui_dir") or "").strip()
        sync_mode = normalize_node_sync_mode(node)
        item: dict[str, Any] = {
            "node_id": node_id,
            "hostname": hostname,
            "role": role,
            "sync_mode": sync_mode,
            "ssh_host": ssh_host,
            "ssh_user": ssh_user,
            "ssh_port": ssh_port,
            "remote_pbgui_dir": remote_pbgui_dir,
            "action": "skip",
            "reason": "",
            "changes": [],
            "hosts_action": "none",
            "hosts_current_ip": "",
        }
        if node_id == local_node_id:
            item.update({"reason": "local Cluster node"})
            return item
        if not node_id:
            item.update({"action": "error", "reason": "Cluster node has no node_id."})
            return item
        try:
            _validate_import_hostname(hostname)
        except ValueError as exc:
            item.update({"action": "error", "reason": str(exc)})
            return item
        if not ssh_host:
            item.update({"reason": "Cluster node has no ssh_host metadata."})
            return item
        if not ssh_user:
            item.update({"action": "error", "reason": "Cluster node has no ssh_user metadata."})
            return item
        hosts_status = _hosts_entry_status(hostname, ssh_host)
        if not hosts_status.get("ok"):
            item["hosts_action"] = "replace" if hosts_status.get("has_hostname") else "add"
            item["hosts_current_ip"] = str(hosts_status.get("current_ip") or "")
        current = existing.get(hostname)
        if current is None:
            item.update({"action": "add", "reason": "VPS Manager host is missing."})
            return item
        changes: list[str] = []
        comparisons = (
            ("ip", str(getattr(current, "ip", "") or ""), ssh_host),
            ("user", str(getattr(current, "user", "") or ""), ssh_user),
            ("firewall_ssh_port", str(_safe_int(getattr(current, "firewall_ssh_port", 22), 22)), str(ssh_port)),
            ("remote_pbgui_dir", str(getattr(current, "remote_pbgui_dir", "") or ""), remote_pbgui_dir),
        )
        for field_name, old_value, new_value in comparisons:
            if new_value and old_value != new_value:
                changes.append(field_name)
        host_finalized = True
        if monitor_state is not None:
            host_state = self._get_host_telemetry(monitor_state, hostname)
            host_finalized = self._host_online(host_state) and self._host_telemetry_fresh(host_state)
        if changes:
            item.update({"action": "update", "reason": "Safe VPS Manager metadata differs.", "changes": changes})
        elif item.get("hosts_action") != "none":
            item.update({"action": "update", "reason": "Local /etc/hosts entry needs updating."})
        elif not host_finalized:
            item.update({"action": "update", "reason": "VPS Manager host needs finalization; no current live telemetry."})
        else:
            item.update({"reason": "VPS Manager host already matches safe Cluster metadata."})
        return item

    def preview_cluster_nodes_import(self) -> dict[str, Any]:
        """Preview Cluster nodes that can become local VPS Manager hosts."""

        self._sync_vps_inventory()
        nodes, local_node_id = self._cluster_nodes_for_vps_import()
        existing = {str(item.hostname or "").strip(): item for item in self.vpsmanager.vpss if str(item.hostname or "").strip()}
        monitor_state = self._get_monitor_state()
        items = [self._cluster_node_vps_import_item(node, local_node_id, existing, monitor_state) for node in nodes]
        counts: dict[str, int] = {"add": 0, "update": 0, "skip": 0, "error": 0}
        for item in items:
            action = str(item.get("action") or "skip")
            counts[action] = counts.get(action, 0) + 1
        hosts_update_count = sum(
            1
            for item in items
            if str(item.get("action") or "") in {"add", "update"}
            and str(item.get("hosts_action") or "none") != "none"
        )
        counts["hosts_update"] = hosts_update_count
        return {
            "items": items,
            "counts": counts,
            "can_apply": bool(counts.get("add") or counts.get("update")) and not bool(counts.get("error")),
            "hosts_update_required": bool(hosts_update_count),
            "message": "Imports Cluster nodes with SSH metadata into local VPS Manager metadata only. Secrets are not imported.",
        }

    def _prune_cluster_import_jobs(self) -> None:
        cutoff = time.time() - CLUSTER_IMPORT_JOB_TTL_SECONDS
        with self._cluster_import_jobs_lock:
            stale = [
                job_id
                for job_id, job in self._cluster_import_jobs.items()
                if float(job.get("updated_at") or job.get("started_at") or 0) < cutoff
            ]
            for job_id in stale:
                self._cluster_import_jobs.pop(job_id, None)

    def _cluster_import_job_snapshot(self, job: dict[str, Any]) -> dict[str, Any]:
        events = list(job.get("events") or [])
        return {
            "job_id": str(job.get("job_id") or ""),
            "status": str(job.get("status") or "queued"),
            "label": str(job.get("label") or ""),
            "hostname": str(job.get("hostname") or ""),
            "done": int(job.get("done") or 0),
            "total": int(job.get("total") or 1),
            "percent": int(job.get("percent") or 0),
            "events": events[-60:],
            "result": job.get("result"),
            "error": str(job.get("error") or ""),
            "started_at": float(job.get("started_at") or 0),
            "updated_at": float(job.get("updated_at") or 0),
        }

    def _update_cluster_import_job(self, job_id: str, update: dict[str, Any]) -> dict[str, Any]:
        now = time.time()
        with self._cluster_import_jobs_lock:
            job = self._cluster_import_jobs.get(job_id)
            if job is None:
                raise ValueError("Cluster import job not found.")
            label = str(update.get("label") or "").strip()
            hostname = str(update.get("hostname") or "").strip()
            status = str(update.get("status") or job.get("status") or "running")
            for key in ("status", "label", "hostname", "done", "total", "percent", "result", "error"):
                if key in update:
                    job[key] = update[key]
            job["updated_at"] = now
            if label:
                events = list(job.get("events") or [])
                events.append({
                    "ts": now,
                    "status": status,
                    "label": label,
                    "hostname": hostname,
                    "done": int(update.get("done") if "done" in update else job.get("done") or 0),
                    "total": int(update.get("total") if "total" in update else job.get("total") or 1),
                    "percent": int(update.get("percent") if "percent" in update else job.get("percent") or 0),
                })
                job["events"] = events[-80:]
            return self._cluster_import_job_snapshot(job)

    def get_cluster_nodes_import_progress(self, job_id: str) -> dict[str, Any]:
        self._prune_cluster_import_jobs()
        with self._cluster_import_jobs_lock:
            job = self._cluster_import_jobs.get(str(job_id or "").strip())
            if job is None:
                raise ValueError("Cluster import job not found.")
            return self._cluster_import_job_snapshot(job)

    def _run_cluster_nodes_import_job(self, job_id: str, token: str, form: dict[str, Any]) -> None:
        self._update_cluster_import_job(job_id, {
            "status": "running",
            "label": "Preparing Cluster node import...",
            "done": 0,
            "total": 1,
            "percent": 0,
        })
        try:
            result = self.import_cluster_nodes(
                token,
                form,
                progress=lambda update: self._update_cluster_import_job(job_id, update),
            )
            self._update_cluster_import_job(job_id, {
                "status": "successful",
                "label": str(result.get("message") or "Cluster node import completed."),
                "percent": 100,
                "result": result,
                "error": "",
            })
        except Exception as exc:
            _log(SERVICE, f"Cluster node import job failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
            self._update_cluster_import_job(job_id, {
                "status": "error",
                "label": str(exc) or "Cluster node import failed.",
                "percent": 100,
                "error": str(exc) or "Cluster node import failed.",
            })

    def start_cluster_nodes_import(self, token: str, form: dict[str, Any] | None = None) -> dict[str, Any]:
        """Start Cluster node import in the background and return its progress handle."""

        self._prune_cluster_import_jobs()
        job_id = secrets.token_urlsafe(12)
        now = time.time()
        with self._cluster_import_jobs_lock:
            self._cluster_import_jobs[job_id] = {
                "job_id": job_id,
                "status": "queued",
                "label": "Queued Cluster node import...",
                "hostname": "",
                "done": 0,
                "total": 1,
                "percent": 0,
                "events": [],
                "result": None,
                "error": "",
                "started_at": now,
                "updated_at": now,
            }
        thread = threading.Thread(
            target=self._run_cluster_nodes_import_job,
            args=(job_id, token, dict(form or {})),
            name=f"cluster-import-{job_id[:8]}",
            daemon=True,
        )
        thread.start()
        return self.get_cluster_nodes_import_progress(job_id)

    def import_cluster_nodes(
        self,
        token: str,
        form: dict[str, Any] | None = None,
        progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Import safe Cluster SSH metadata into VPS Manager host configs."""

        form = form or {}
        progress_cb = progress if callable(progress) else None
        done_steps = 0
        total_steps = 1

        def emit_progress(label: str, *, hostname: str = "", status: str = "running", advance: bool = False) -> None:
            nonlocal done_steps
            if advance:
                done_steps = min(total_steps, done_steps + 1)
            if progress_cb is None:
                return
            percent = int(round((done_steps / max(1, total_steps)) * 100))
            progress_cb({
                "status": status,
                "label": label,
                "hostname": hostname,
                "done": done_steps,
                "total": total_steps,
                "percent": max(0, min(100, percent)),
            })

        emit_progress("Building Cluster node import plan...")
        plan = self.preview_cluster_nodes_import()
        if plan.get("counts", {}).get("error"):
            raise ValueError("Fix Cluster node import errors before applying.")
        import_items = [
            item
            for item in (plan.get("items") or [])
            if str((item or {}).get("action") or "") in {"add", "update"}
        ]
        passwords_raw = form.get("passwords") if isinstance(form.get("passwords"), dict) else {}
        passwords = {
            str(key or "").strip(): str(value or "")
            for key, value in passwords_raw.items()
            if str(key or "").strip() and str(value or "")
        }
        selected_import_items = [
            item
            for item in import_items
            if passwords.get(str((item or {}).get("hostname") or "").strip())
        ]
        if not selected_import_items:
            raise ValueError("Enter the VPS user password for at least one Cluster node to import.")
        total_steps = (len(selected_import_items) * 5) + 2
        emit_progress(f"Prepared {len(selected_import_items)} selected Cluster node(s).", advance=True)
        hosts_items = [
            item
            for item in selected_import_items
            if str((item or {}).get("hosts_action") or "none") != "none"
        ]
        local_sudo_pw = str(form.get("local_sudo_pw") or "")
        if hosts_items and not local_sudo_pw:
            raise ValueError("Local sudo password is required to update /etc/hosts for imported Cluster nodes.")
        existing = {str(item.hostname or "").strip(): item for item in self.vpsmanager.vpss if str(item.hostname or "").strip()}
        imported: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        hosts_updated: list[dict[str, Any]] = []
        settings_refreshed: list[str] = []
        monitoring_ready: list[str] = []
        warnings: list[str] = []
        for item in plan.get("items") or []:
            action = str((item or {}).get("action") or "skip")
            hostname = str((item or {}).get("hostname") or "").strip()
            if action not in {"add", "update"}:
                skipped.append({"hostname": hostname, "action": action, "reason": str((item or {}).get("reason") or "")})
                continue
            host_password = passwords.get(hostname, "")
            if not host_password:
                skipped.append({"hostname": hostname, "action": "skip", "reason": "No VPS user password entered."})
                emit_progress("Skipped because no VPS user password was entered.", hostname=hostname, status="skipped")
                continue
            if str((item or {}).get("hosts_action") or "none") != "none":
                emit_progress("Updating local /etc/hosts entry...", hostname=hostname)
                hosts_result = self.write_hosts_entry(str((item or {}).get("ssh_host") or ""), hostname, local_sudo_pw)
                if not hosts_result.get("ok"):
                    raise ValueError(f"Failed to update /etc/hosts for {hostname}: {hosts_result.get('error') or 'unknown error'}")
                hosts_updated.append({"hostname": hostname, "ip": str((item or {}).get("ssh_host") or ""), "action": str((item or {}).get("hosts_action") or "")})
                emit_progress("Updated local /etc/hosts entry.", hostname=hostname, status="done", advance=True)
            else:
                emit_progress("No local /etc/hosts change needed.", hostname=hostname, status="done", advance=True)
            vps = existing.get(hostname)
            is_new = vps is None
            emit_progress("Writing safe VPS Manager metadata...", hostname=hostname)
            if vps is None:
                vps = VPS()
                vps.hostname = hostname
                vps.swap = "0"
                vps.firewall = False
                vps.init_methode = "password"
                vps.init_status = "successful"
                vps.setup_status = "successful"
                vps.command = "import-cluster-node"
                vps.command_text = "Imported from Cluster Nodes"
            vps.ip = str((item or {}).get("ssh_host") or vps.ip or "").strip()
            if str((item or {}).get("ssh_user") or "").strip():
                vps.user = str((item or {}).get("ssh_user") or "").strip()
            vps.firewall_ssh_port = _safe_int((item or {}).get("ssh_port"), 22) or 22
            if str((item or {}).get("remote_pbgui_dir") or "").strip():
                vps.remote_pbgui_dir = str((item or {}).get("remote_pbgui_dir") or "").strip()
            emit_progress("Wrote safe VPS Manager metadata.", hostname=hostname, status="done", advance=True)
            if host_password:
                vps.user_pw = host_password
                self._store_session_secrets(token, hostname, {"user_pw": host_password})
                emit_progress("Refreshing remote settings...", hostname=hostname)
                try:
                    info = vps.fetch_vps_info()
                    vps.coinmarketcap_api_key = str(info.get("coinmarketcap") or vps.coinmarketcap_api_key or "")
                    vps.swap = info.get("swap", "0") if info.get("swap") in SWAP_OPTIONS else str(vps.swap or "0")
                    if info.get("firewall_ssh_port") is not None:
                        vps.firewall_ssh_port = _safe_int(info.get("firewall_ssh_port"), 22)
                    vps.firewall, vps.firewall_ssh_ips = vps.fetch_ufw_settings()
                    self._clear_vps_optional_config_pending(vps)
                    settings_refreshed.append(hostname)
                    emit_progress("Refreshed remote settings.", hostname=hostname, status="done", advance=True)
                except Exception as exc:
                    warnings.append(f"{hostname}: settings refresh failed: {exc}")
                    emit_progress(f"Settings refresh failed: {exc}", hostname=hostname, status="warning", advance=True)
                emit_progress("Checking monitoring SSH key...", hostname=hostname)
                key_auth_ok, key_auth_detail = self._test_import_key_login(ssh_host=vps.ip, user=vps.user)
                if not key_auth_ok:
                    emit_progress("Installing monitoring SSH key...", hostname=hostname)
                    key_auth_ok, key_auth_detail = self._install_import_monitoring_key(ssh_host=vps.ip, user=vps.user, user_pw=host_password)
                if key_auth_ok:
                    monitoring_ready.append(hostname)
                    emit_progress("Monitoring SSH key is ready.", hostname=hostname, status="done", advance=True)
                else:
                    warnings.append(f"{hostname}: monitoring key setup failed: {key_auth_detail}")
                    emit_progress(f"Monitoring key setup failed: {key_auth_detail}", hostname=hostname, status="warning", advance=True)
                vps.user_pw = None
            else:
                vps.user_pw = None
            emit_progress("Saving VPS Manager host entry...", hostname=hostname)
            vps.save()
            if is_new:
                self.vpsmanager.vpss.append(vps)
                existing[hostname] = vps
            if hostname in monitoring_ready:
                self._set_vps_monitor_enabled(hostname, enabled=True)
                self._refresh_vps_monitor_connection(hostname)
            imported.append({"hostname": hostname, "action": action, "monitoring_ready": hostname in monitoring_ready, "settings_refreshed": hostname in settings_refreshed, "config": self._build_vps_config(token, vps)})
            emit_progress("Saved VPS Manager host entry.", hostname=hostname, status="done", advance=True)
        self.vpsmanager.vpss.sort(key=lambda entry: entry.hostname or "")
        emit_progress("Finalizing Cluster node import...", status="done", advance=True)
        return {
            "ok": True,
            "counts": {
                "imported": len(imported),
                "skipped": len(skipped),
                "hosts_updated": len(hosts_updated),
                "settings_refreshed": len(settings_refreshed),
                "monitoring_ready": len(monitoring_ready),
            },
            "imported": imported,
            "skipped": skipped,
            "hosts_updated": hosts_updated,
            "settings_refreshed": settings_refreshed,
            "monitoring_ready": monitoring_ready,
            "warnings": warnings,
            "preview": plan,
            "message": f"Imported {len(imported)} selected Cluster node(s) into VPS Manager" + (f" and updated {len(hosts_updated)} /etc/hosts entr{'y' if len(hosts_updated) == 1 else 'ies'}." if hosts_updated else "."),
        }

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

    def _vps_optional_config_pending_path(self, vps: VPS) -> Path | None:
        hostname = str(getattr(vps, "hostname", "") or "").strip()
        if not hostname:
            return None
        base_path = getattr(vps, "path", None)
        if base_path:
            return Path(base_path) / "optional_config_pending.json"
        return Path(PBGDIR) / "data" / "vpsmanager" / "hosts" / hostname / "optional_config_pending.json"

    def _load_vps_optional_config_pending(self, vps: VPS) -> dict[str, str]:
        path = self._vps_optional_config_pending_path(vps)
        if path is None or not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        if not isinstance(payload, dict):
            return {}
        return {
            field: str(payload.get(field) or "").strip()
            for field in OPTIONAL_VPS_CONFIG_FIELDS
            if field in payload
        }

    def _write_vps_optional_config_pending(self, vps: VPS, values: dict[str, Any]) -> None:
        path = self._vps_optional_config_pending_path(vps)
        if path is None:
            return
        payload = {
            field: str(values.get(field) or "").strip()
            for field in OPTIONAL_VPS_CONFIG_FIELDS
            if field in values
        }
        if not payload:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            return
        payload["updated_at"] = datetime.now().isoformat(timespec="seconds")
        _atomic_write_json(path, payload)

    def _clear_vps_optional_config_pending(self, vps: VPS) -> None:
        self._write_vps_optional_config_pending(vps, {})

    def _sync_vps_config_from_host_meta(self, vps: VPS, host_state: dict[str, Any] | None) -> None:
        if not vps or not self._host_telemetry_fresh(host_state):
            return
        meta = self._host_meta(host_state)
        pending = self._load_vps_optional_config_pending(vps)
        remaining_pending = dict(pending)
        changed = False

        def sync_optional_field(meta_key: str, attr_name: str, pending_key: str) -> None:
            nonlocal changed
            if meta_key not in meta:
                return
            remote_value = str(meta.get(meta_key) or "").strip()
            if pending_key in pending:
                desired_value = str(pending.get(pending_key) or "").strip()
                if str(getattr(vps, attr_name, "") or "") != desired_value:
                    setattr(vps, attr_name, desired_value)
                    changed = True
                if remote_value == desired_value:
                    remaining_pending.pop(pending_key, None)
                return
            if str(getattr(vps, attr_name, "") or "") != remote_value:
                setattr(vps, attr_name, remote_value)
                changed = True

        if "coinmarketcap_api_key" in meta:
            sync_optional_field("coinmarketcap_api_key", "coinmarketcap_api_key", "coinmarketcap_api_key")
        if _truthy(meta.get("firewall_settings_present")):
            remote_firewall = _truthy(meta.get("firewall"))
            remote_firewall_port = _safe_int(meta.get("firewall_ssh_port"), 22)
            remote_firewall_ips = str(meta.get("firewall_ssh_ips") or "").strip()
            if bool(getattr(vps, "firewall", False)) != remote_firewall:
                vps.firewall = remote_firewall
                changed = True
            if _safe_int(getattr(vps, "firewall_ssh_port", 22), 22) != remote_firewall_port:
                vps.firewall_ssh_port = remote_firewall_port
                changed = True
            if str(getattr(vps, "firewall_ssh_ips", "") or "").strip() != remote_firewall_ips:
                vps.firewall_ssh_ips = remote_firewall_ips
                changed = True
        if remaining_pending != pending:
            self._write_vps_optional_config_pending(vps, remaining_pending)
        if changed:
            vps.save()

    def _refresh_vps_instances_now(self, hostname: str) -> None:
        monitor = get_monitor()
        if monitor is None:
            return
        host = str(hostname or "").strip()
        if not host:
            return
        try:
            loop = getattr(monitor, "loop", None)
            if loop is None or loop.is_closed():
                return
            asyncio.run_coroutine_threadsafe(monitor.collect_instances_now(host), loop).result(timeout=30)
        except Exception as exc:
            _log(SERVICE, f"immediate instance refresh failed for {host}: {exc}", level="WARNING")

    def _refresh_vps_monitor_connection(self, hostname: str) -> None:
        monitor = get_monitor()
        if monitor is None or not hasattr(monitor, "refresh_enabled_host"):
            return
        host = str(hostname or "").strip()
        if not host:
            return
        try:
            loop = getattr(monitor, "loop", None)
            if loop is None or loop.is_closed():
                return
            asyncio.run_coroutine_threadsafe(monitor.refresh_enabled_host(host), loop).result(timeout=30)
        except Exception as exc:
            _log(SERVICE, f"monitor reconnect failed for {host}: {exc}", level="WARNING")

    def _local_v7_dynamic_ignore_enabled_on(self, name: str, hostname: str) -> bool:
        safe_name = str(name or "").strip()
        host = str(hostname or "").strip()
        if not safe_name or not host or safe_name in {".", ".."} or any(ch in safe_name for ch in ("/", "\\", "\x00")):
            return False
        config_path = Path(PBGDIR) / "data" / "run_v7" / safe_name / "config.json"
        if not config_path.is_file():
            return False
        try:
            cfg = json.loads(config_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return False
        pbgui = cfg.get("pbgui") if isinstance(cfg, dict) else None
        if not isinstance(pbgui, dict) or not bool(pbgui.get("dynamic_ignore")):
            return False
        enabled_on = str(pbgui.get("enabled_on") or "").strip()
        return not enabled_on or enabled_on == host

    def _running_dynamic_ignore_v7_bots(self, hostname: str, host_state: dict[str, Any] | None) -> list[str]:
        host = str(hostname or "").strip()
        out: list[str] = []
        for instance in (host_state or {}).get("v7_instances") or []:
            if not _truthy((instance or {}).get("running")):
                continue
            name = str((instance or {}).get("name") or "").strip()
            if not name:
                continue
            if _truthy((instance or {}).get("di")) or _truthy((instance or {}).get("dynamic_ignore")):
                out.append(name)
                continue
            if self._local_v7_dynamic_ignore_enabled_on(name, host):
                out.append(name)
        return sorted(dict.fromkeys(out))

    def _ensure_coinmarketcap_key_clear_allowed(self, vps: VPS, next_key: str) -> None:
        if _configured_optional_secret(next_key) or not _configured_optional_secret(vps.coinmarketcap_api_key):
            return
        hostname = str(vps.hostname or "").strip()
        if not hostname:
            return
        self._refresh_vps_instances_now(hostname)
        monitor_state = self._get_monitor_state()
        host_state = self._get_host_telemetry(monitor_state, hostname)
        running_dynamic = self._running_dynamic_ignore_v7_bots(hostname, host_state)
        if not running_dynamic:
            return
        names = ", ".join(running_dynamic[:5])
        suffix = "" if len(running_dynamic) <= 5 else f" and {len(running_dynamic) - 5} more"
        raise ValueError(
            "Cannot remove the CoinMarketCap API key while dynamic_ignore bot(s) "
            f"are running on {hostname}: {names}{suffix}. Stop them first."
        )

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

    def build_state(self) -> dict[str, Any]:
        self.refresh(force=False)
        monitor_state = self._get_monitor_state()
        overview_rows = self._build_overview_rows(monitor_state)
        coindata = self._ensure_coindata()
        cmc_api_key = coindata.api_key or ""
        vps_logging = self.get_vps_logging_config()
        deploy_settings = self.get_vps_deploy_settings()
        deploy_history = self.get_vps_deploy_history()
        deploy_progress_rows = self._build_deploy_progress_rows(overview_rows, deploy_history)
        return {
            "config": {
                "master_name": _local_master_name(),
                "local_user": getpass.getuser(),
                "swap_options": SWAP_OPTIONS,
                "init_methods": INIT_METHODS,
                "coinmarketcap_api_key": cmc_api_key,
                "vps_logging": vps_logging,
                "vps_deploy": deploy_settings,
            },
            "errors": [],
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
            host_log_status = str(host_log_meta.get("status") or "").strip().lower()
            host_log_reason = str(host_log_meta.get("reason") or "").strip()

            if host_log_status == "skipped":
                progress_rows.append(
                    {
                        **base_row,
                        "task_command": latest_command,
                        "task_command_text": latest_command_text,
                        "task_status": "skipped",
                        "task_started": expected_started_at or latest_started_at,
                        "task_step": "",
                        "task_step_kind": "",
                        "task_result": "skipped",
                        "task_recap": None,
                        "task_current_index": 0,
                        "task_total_steps": 0,
                        "task_current_label": host_log_reason or "Not started: active VPS task already running",
                        "task_progress": {"done": 0, "total": 0, "percent": 0},
                        "task_log_file": file_alias,
                        "task_log_filename": filename,
                    }
                )
                continue

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
        coindata = self._ensure_coindata()
        coindata_ok = False
        try:
            coindata_ok = coindata.fetch_api_status()
        except Exception:
            coindata_ok = False
        self._master_coindata_ok_cache = bool(coindata_ok)
        master_monitor = self._build_local_master_monitor_payload(refresh=True)
        return {
            "kind": "master",
            "status": self._build_master_status(coindata_ok),
            "branches": {
                "pbgui": self._build_master_pbgui_branch_state(),
                "pb7": self._build_master_pb7_branch_state(),
            },
            "monitor": master_monitor,
            "progress": self._build_master_progress(include_log=True),
        }

    def build_master_detail_quick(self) -> dict[str, Any]:
        return {
            "kind": "master",
            # Quick detail must not overwrite validated full-detail status with
            # a cheap fallback such as a hardcoded False.
            "status": self._build_master_status(self._master_coindata_ok_cache),
            "branches": {
                "pbgui": self._build_master_pbgui_branch_state(),
                "pb7": self._build_master_pb7_branch_state(),
            },
            "monitor": self._build_local_master_monitor_payload(refresh=False),
            "progress": self._build_master_progress(include_log=True),
        }

    def build_vps_detail(self, token: str, hostname: str, *, quick: bool = False) -> dict[str, Any]:
        if not quick:
            self.refresh(force=False)
        vps = self._require_vps(hostname)
        if not quick and str(getattr(vps, "update_status", "") or "").strip().lower() in {"successful", "failed", "error", "timeout", "canceled", "cancelled", "unreachable"}:
            self._refresh_host_meta_now(hostname)
        self._apply_session_secrets_to_vps(token, vps)
        monitor_state = self._get_monitor_state()
        host_state = self._get_host_telemetry(monitor_state, hostname)
        self._sync_vps_config_from_host_meta(vps, host_state)
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
            "status": self._build_vps_status(vps, host_state, coindata_ok, quick=quick),
            "config": self._build_vps_config(token, vps),
            "branches": {
                "pbgui": self._build_vps_pbgui_branch_state(host_state),
                "pb7": self._build_vps_pb7_branch_state(host_state, hostname),
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
        if hostname != _local_master_name():
            self._require_vps(hostname)
        return get_metric_history_snapshot(hostname, bot_name=bot_name, metric=metric)

    def _build_overview_rows(self, monitor_state: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = [self._build_master_overview_row()]
        managed_hostnames: set[str] = set()
        for vps in sorted(self.vpsmanager.vpss, key=lambda item: item.hostname or ""):
            hostname = str(vps.hostname or "")
            if not hostname:
                continue
            managed_hostnames.add(hostname)
            host_state = self._get_host_telemetry(monitor_state, hostname)
            self._sync_vps_config_from_host_meta(vps, host_state)
            rows.append(self._build_vps_overview_row(hostname, host_state))
        return rows

    def _build_master_overview_row(self) -> dict[str, Any]:
        pbgui_release = self._get_pbgui_release()
        pb7_release = self._get_pb7_release()
        local_package_status = self._get_local_package_status()
        local_pbgui_python = f"{sys.version_info.major}.{sys.version_info.minor}"
        local_pb7_python = _python_major_minor(load_ini("main", "pb7venv"))
        master_name = _local_master_name()
        master_branch = str(pbgui_release.get("current_branch") or "unknown")
        master_commit = str(pbgui_release.get("current_commit") or "")
        master_pb7_branch = str(pb7_release.get("current_branch") or "unknown")
        master_pb7_commit = str(pb7_release.get("current_commit") or "")
        boot_ts = int(psutil.boot_time() or 0)
        return {
            "name": f"{master_name} (local)",
            "hostname": master_name,
            "nav": "master",
            "online": True,
            "role": "master",
            "role_icon": "🧠",
            "start": datetime.fromtimestamp(boot_ts).strftime("%Y-%m-%d %H:%M:%S"),
            "start_ts": boot_ts,
            "reboot_required": bool(local_package_status.get("reboot", False)),
            "updates": local_package_status.get("upgrades", "N/A"),
            "running_bots": "-",
            "pbgui": f"{str(pbgui_release.get('version') or 'N/A')}{'' if local_pbgui_python in (None, '', 'N/A') else ' /' + str(local_pbgui_python)}",
            "pbgui_branch": f"{master_branch} ({_short_commit(master_commit)})",
            "pbgui_github": self._build_master_pbgui_github_status(master_branch, master_commit),
            "pb7": f"{str(pb7_release.get('version') or 'N/A')}{'' if local_pb7_python in (None, '', 'N/A') else ' /' + str(local_pb7_python)}",
            "pb7_branch": f"{master_pb7_branch} ({_short_commit(master_pb7_commit)})",
            "pb7_github": self._build_master_pb7_github_status(master_pb7_branch, master_pb7_commit),
        }

    def _build_vps_overview_row(self,
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
            "user": str(getattr(vps, "user", "") or "") if vps else "",
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
            "pbgui_github": self._build_remote_pbgui_github_status(host_state),
            "pb7": f"{meta.get('pb7v', 'N/A')}{'' if meta.get('pb7py', 'N/A') in (None, '', 'N/A') else ' /' + str(meta.get('pb7py'))}",
            "pb7_branch": f"{meta.get('pb7b', 'unknown')} ({_short_commit(meta.get('pb7c'))})",
            "pb7_github": self._build_remote_pb7_github_status(host_state),
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

    def _build_master_pbgui_github_status(self, current_branch: str, current_commit: str) -> str:
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

    def _build_master_pb7_github_status(self, current_branch: str, current_commit: str) -> str:
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

    def _build_remote_pbgui_github_status(self, host_state: dict[str, Any]) -> str:
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
            target_version = str(release_info.get("origin_version") or release_info.get("version") or "N/A")
            return f"❌ {target_version} ({_short_commit(origin_commit)})"
        if server_branch == "main":
            if server_version == str(release_info.get("origin_version") or "N/A") and server_commit == str(release_info.get("origin_commit") or ""):
                return "✅"
            return f"❌ {str(release_info.get('origin_version') or 'N/A')} ({_short_commit(str(release_info.get('origin_commit') or ''))})"
        return f"⚠️ {server_version}"

    def _build_remote_pb7_github_status(self, host_state: dict[str, Any]) -> str:
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
            target_version = str(release_info.get("origin_version") or release_info.get("version") or "N/A")
            return f"❌ {target_version} ({_short_commit(origin_commit)})"
        if server_branch == "master":
            if server_version == str(release_info.get("origin_version") or "N/A") and server_commit == str(release_info.get("origin_commit") or ""):
                return "✅"
            return f"❌ {str(release_info.get('origin_version') or 'N/A')} ({_short_commit(str(release_info.get('origin_commit') or ''))})"
        return f"⚠️ {server_version}"

    def _build_master_status(self, coindata_ok: bool) -> dict[str, Any]:
        summary_row = self._build_master_overview_row()
        local_coindata = self._ensure_coindata()
        local_no_new_privs = _local_no_new_privileges()
        local_sudo_blocked_reason = "Local sudo blocked by runtime (`NoNewPrivs`)." if local_no_new_privs else ""
        pbgui_github = str(summary_row.get("pbgui_github") or "")
        pb7_github = str(summary_row.get("pb7_github") or "")
        return {
            "name": _local_master_name(),
            "online": bool(summary_row.get("online")),
            "coindata_ok": coindata_ok,
            "coindata_configured": True,
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
                          coindata_ok: bool, *, quick: bool = False) -> dict[str, Any]:
        hostname = str(vps.hostname or "")
        summary_row = self._build_vps_overview_row(vps.hostname, host_state)
        cluster_node = self._cluster_node_status(hostname)
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
        pbgui_github = self._build_remote_pbgui_github_status(host_state)
        pb7_github = self._build_remote_pb7_github_status(host_state)
        ssh_online = self._host_online(host_state)
        telemetry_fresh = self._host_telemetry_fresh(host_state)
        telemetry_age = self._host_telemetry_age(host_state)
        host_meta = self._host_meta(host_state)
        stream = (host_state or {}).get("stream") or {}
        monitor_agent = stream.get("monitor_agent") if isinstance(stream, dict) else None
        if not isinstance(monitor_agent, dict):
            monitor_agent = {
                "state": "missing" if ssh_online else "unknown",
                "error": "No monitor-agent status has been reported" if ssh_online else "Host is not connected",
            }
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
            "coindata_ok": coindata_ok,
            "coindata_configured": True,
            "cmc_credits": host_meta.get("cmc_credits"),
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
            "systemd_migration": self._get_vps_systemd_migration_status(vps, host_state, quick=quick),
            "cluster_node": cluster_node,
            "monitor_agent": monitor_agent,
        }

    def _cluster_node_status(self, hostname: str) -> dict[str, Any]:
        """Return the local Cluster bootstrap status for one VPS Manager host."""

        host = str(hostname or "").strip()
        if not host:
            return {"ok": False, "registered": False, "action": "error", "reason": "Hostname is required."}
        try:
            from api import cluster

            plan = cluster._build_bootstrap_plan()
        except Exception as exc:
            return {"ok": False, "registered": False, "action": "error", "reason": str(getattr(exc, "detail", None) or exc)}
        for item in plan.get("items", []) if isinstance(plan, dict) else []:
            if str(item.get("type") or "") != "node":
                continue
            if str(item.get("hostname") or item.get("pbname") or "").strip() != host:
                continue
            action = str(item.get("action") or "")
            return {
                "ok": action != "error",
                "registered": action == "skip",
                "action": action,
                "reason": str(item.get("reason") or ""),
                "node_id": str(item.get("node_id") or ""),
            }
        return {"ok": False, "registered": False, "action": "missing", "reason": "VPS host is not known to Cluster bootstrap."}

    def _empty_vps_systemd_migration_status(self, state: str = "unknown", error: str = "") -> dict[str, Any]:
        return {
            "state": state,
            "available": False,
            "migration_complete": False,
            "migration_needed": False,
            "units_ready": False,
            "legacy_process_count": 0,
            "legacy_cron_count": 0,
            "legacy_start_sh_exists": False,
            "required_units": [],
            "units": [],
            "checked_at": 0,
            "error": error,
        }

    def _vps_systemd_migration_task_overlay(self, vps: VPS, base: dict[str, Any] | None = None) -> dict[str, Any] | None:
        command = str(getattr(vps, "command", "") or "").strip()
        if command != COMMAND_VPS_MIGRATE_SYSTEMD:
            return None
        status = str(getattr(vps, "update_status", "") or "").strip().lower()
        failed_states = {"failed", "error", "timeout", "canceled", "cancelled", "unreachable"}
        running_states = {"", "queued", "pending", "starting", "running", "in_progress"}
        if status in failed_states:
            out = dict(base or self._empty_vps_systemd_migration_status("failed"))
            out.update({
                "state": "failed",
                "available": True,
                "migration_complete": False,
                "migration_needed": True,
                "error": "Last systemd migration task failed. Click to preview current state.",
            })
            return out
        if status in running_states:
            out = dict(base or self._empty_vps_systemd_migration_status("running"))
            out.update({
                "state": "running",
                "available": True,
                "migration_complete": False,
                "migration_needed": True,
                "error": "",
            })
            return out
        return None

    def _build_vps_systemd_migration_status_from_preview(self, parsed: dict[str, Any]) -> dict[str, Any]:
        values = parsed.get("values") or {}
        units = parsed.get("units") or []
        cron_lines = parsed.get("cron") or []
        processes = parsed.get("processes") or []
        pbgui_exists = values.get("pbgui_dir_exists") == "yes"
        python_exists = values.get("python_exists") == "yes"
        systemctl_exists = values.get("systemctl_exists") == "yes"
        user_manager_ok = values.get("systemd_user_manager") == "yes"
        start_sh_exists = values.get("start_sh_exists") == "yes"
        required_units = self._systemd_migration_required_units(units, values)
        units_missing = [item for item in required_units if item.get("exists") != "yes"]
        units_not_enabled = [item for item in required_units if item.get("enabled") != "enabled"]
        units_inactive = [item for item in required_units if item.get("active") != "active"]
        units_ready = bool(required_units) and not units_missing and not units_not_enabled and not units_inactive
        blockers = []
        if not pbgui_exists:
            blockers.append("PBGui directory missing")
        if not python_exists:
            blockers.append("PBGui virtualenv Python missing")
        if not systemctl_exists:
            blockers.append("systemctl missing")
        migration_complete = bool(pbgui_exists and python_exists and systemctl_exists and user_manager_ok and units_ready and not processes and not cron_lines and not start_sh_exists)
        state = "complete" if migration_complete else "blocked" if blockers else "needed"
        return {
            "state": state,
            "available": True,
            "migration_complete": migration_complete,
            "migration_needed": not migration_complete,
            "units_ready": units_ready,
            "required_units": required_units,
            "units": units,
            "legacy_process_count": len(processes),
            "legacy_cron_count": len(cron_lines),
            "legacy_start_sh_exists": start_sh_exists,
            "checked_at": int(time.time()),
            "error": "",
            "blockers": blockers,
        }

    def _systemd_migration_required_units(self, units: list[dict[str, str]], values: dict[str, str]) -> list[dict[str, str]]:
        """Return systemd units required for currently configured optional services."""
        pbrun_configured = values.get("pbrun_configured") == "yes"
        pbdata_configured = values.get("pbdata_configured") == "yes"
        required: list[dict[str, str]] = []
        for item in units:
            unit = item.get("unit")
            if unit == "pbgui-pbrun.service" and not pbrun_configured:
                continue
            if unit == "pbgui-pbdata.service" and not pbdata_configured:
                continue
            required.append(item)
        return required

    def _get_vps_systemd_migration_status(self, vps: VPS, host_state: dict[str, Any], *, quick: bool = False) -> dict[str, Any]:
        hostname = str(vps.hostname or "").strip()
        self._vps_systemd_migration_status_cache.pop(hostname, None)
        meta = self._host_meta(host_state)
        raw_status = meta.get("systemd_migration") if isinstance(meta, dict) else None
        status = dict(raw_status) if isinstance(raw_status, dict) else self._empty_vps_systemd_migration_status("unknown", "Monitor has not reported systemd migration status yet.")
        status.setdefault("available", isinstance(raw_status, dict))
        status.setdefault("migration_complete", False)
        status.setdefault("migration_needed", False)
        status.setdefault("units_ready", False)
        status.setdefault("legacy_process_count", 0)
        status.setdefault("legacy_cron_count", 0)
        status.setdefault("legacy_start_sh_exists", False)
        status.setdefault("required_units", [])
        status.setdefault("units", [])
        status.setdefault("checked_at", 0)
        status.setdefault("error", "")
        task_overlay = self._vps_systemd_migration_task_overlay(vps, status)
        if task_overlay is not None:
            return task_overlay
        if quick:
            return status
        if not hostname:
            return self._empty_vps_systemd_migration_status("unknown", "Hostname missing.")
        if not self._host_online(host_state) and not isinstance(raw_status, dict):
            return self._empty_vps_systemd_migration_status("unknown", "Monitor is offline or has no host metadata yet.")
        return status

    def build_vps_status_with_session(self, token: str, hostname: str, *, quick: bool = False) -> dict[str, Any]:
        vps = self._require_vps(hostname)
        self._apply_session_secrets_to_vps(token, vps)
        monitor_state = self._get_monitor_state()
        host_state = self._get_host_telemetry(monitor_state, hostname)
        self._sync_vps_config_from_host_meta(vps, host_state)
        coindata_ok = bool(self._vps_coindata_status_cache.get(hostname, False)) if quick else False
        return self._build_vps_status(vps, host_state, coindata_ok, quick=quick)

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

    def _local_master_cpu_stats(self) -> tuple[float, float, float]:
        """Return local master live CPU, 60s CPU, and 60s sample window."""
        try:
            cpu_times = psutil.cpu_times()
        except Exception as exc:
            _log(SERVICE, f"local master CPU probe failed: {exc}", level="WARNING")
            return 0.0, 0.0, 0.0

        now = time.time()
        idle = _safe_float(getattr(cpu_times, "idle", 0.0))
        total = sum(_safe_float(value) for value in cpu_times)
        if total <= 0:
            return 0.0, 0.0, 0.0

        history = self._master_server_cpu_history
        cutoff = now - (ROLLING_PEAK_WINDOW_SECONDS + 2.0)
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
            if now - sample[0] >= ROLLING_PEAK_WINDOW_SECONDS:
                base_sample = sample
            else:
                break

        if base_sample is None:
            window = round(now - history[0][0], 1) if history else 0.0
            return round(max(0.0, min(live_cpu, 100.0)), 1), 0.0, window

        elapsed = now - base_sample[0]
        total_delta = total - base_sample[2]
        if elapsed <= 0 or total_delta <= 0:
            return round(max(0.0, min(live_cpu, 100.0)), 1), 0.0, round(max(elapsed, 0.0), 1)
        cpu_60s = (1.0 - ((idle - base_sample[1]) / total_delta)) * 100.0
        return (
            round(max(0.0, min(live_cpu, 100.0)), 1),
            round(max(0.0, min(cpu_60s, 100.0)), 1),
            round(elapsed, 1),
        )

    def _record_local_master_server_metric_history(self, hostname: str, payload: dict[str, Any] | None) -> None:
        hostname = str(hostname or "").strip()
        if not hostname or not payload:
            return
        monitor = get_monitor()
        stores = getattr(monitor, "_host_metric_history", None) if monitor else None
        if not isinstance(stores, dict):
            return
        minute = int(time.time() // 60)
        try:
            cpu_store = stores.get("cpu")
            if cpu_store is not None:
                cpu_store.record(
                    hostname,
                    minute=minute,
                    value=payload.get("cpu_60s"),
                    confirmed=_safe_float(payload.get("cpu_60s_window")) >= ROLLING_PEAK_WINDOW_SECONDS,
                )
                cpu_store.maybe_flush()
            for metric, payload_key in (("memory", "mem"), ("disk", "disk"), ("swap", "swap")):
                store = stores.get(metric)
                data = payload.get(payload_key) if isinstance(payload.get(payload_key), dict) else {}
                if store is None:
                    continue
                store.record(
                    hostname,
                    minute=minute,
                    value=(data or {}).get("usage_pct"),
                    confirmed=_safe_float((data or {}).get("total_mb")) > 0.0,
                    same_minute_mode="peak",
                )
                store.maybe_flush()
        except Exception as exc:
            _log(SERVICE, f"local master history record failed: {exc}", level="WARNING")

    def _build_local_master_server_metrics(self, hostname: str = "") -> dict[str, Any] | None:
        """Build current local master server telemetry without remote daemon probes."""
        try:
            mem = psutil.virtual_memory()
            disk = psutil.disk_usage("/")
            swap = psutil.swap_memory()
            boot = psutil.boot_time()
        except Exception as exc:
            _log(SERVICE, f"local master server telemetry failed: {exc}", level="WARNING")
            return None
        cpu, cpu_60s, cpu_60s_window = self._local_master_cpu_stats()
        payload = self._build_server_metrics(SimpleNamespace(
            rtd=0,
            boot=boot,
            cpu=cpu,
            cpu_60s=cpu_60s,
            cpu_60s_window=cpu_60s_window,
            mem=mem,
            disk=disk,
            swap=swap,
        ))
        self._record_local_master_server_metric_history(hostname, payload)
        return payload

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
                    "blocked": _truthy(instance.get("blocked", False)),
                    "blocked_reason": str(instance.get("blocked_reason") or ""),
                    "cluster_gate": str(instance.get("cluster_gate") or ""),
                }
            )
        items.sort(key=lambda item: item["name"])
        return items

    def _build_local_master_monitor_payload(self, *, refresh: bool) -> dict[str, Any]:
        master_name = _local_master_name()
        if not refresh and self._master_monitor_payload_cache is not None:
            self._master_monitor_payload_cache["server"] = self._build_local_master_server_metrics(master_name)
            return self._master_monitor_payload_cache
        payload = self._empty_monitor_payload()
        payload["server"] = self._build_local_master_server_metrics(master_name)
        snapshot = self._collect_local_master_monitor_snapshot()
        live_stats = self._collect_local_master_live_bot_stats()
        cfg = self.monitor_config
        for monitor in snapshot.get("monitors") or []:
            name = str(monitor.get("u") or "")
            live = live_stats.get(name) or {}
            start_ts = _safe_int(monitor.get("st"))
            pnl_hist_total, pnls_hist_total = self._bot_pnl_total(master_name, name)
            item = {
                "server": master_name,
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
                "errors_4w": self._bot_count_total(master_name, name, "errors"),
                "tracebacks_today": _safe_int(monitor.get("tt")),
                "tracebacks_4w": self._bot_count_total(master_name, name, "tracebacks"),
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

    def _build_master_pbgui_branch_state(self) -> dict[str, Any]:
        release_info = self._get_pbgui_release()
        current_branch = str(release_info.get("current_branch") or "unknown")
        current_commit = str(release_info.get("current_commit") or "")
        return {
            "current_branch": current_branch,
            "current_commit": current_commit,
            "branches": release_info.get("branches") or {},
        }

    def _build_master_pb7_branch_state(self) -> dict[str, Any]:
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

    def _build_vps_pbgui_branch_state(self, host_state: dict[str, Any]) -> dict[str, Any]:
        meta = self._host_meta(host_state)
        return {
            "current_branch": str(meta.get("pbgb") or "unknown"),
            "current_commit": str(meta.get("pbgc") or ""),
            "branches": self._get_pbgui_release().get("branches") or {},
        }

    def _build_vps_pb7_branch_state(self,
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
            "install_dir": _install_dir_from_remote_pbgui_dir(vps.remote_pbgui_dir, vps.user),
            "remote_pbgui_dir": vps.remote_pbgui_dir or "",
            "swap": vps.swap or "0",
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
                "status": str(payload.get("status") or ""),
                "reason": str(payload.get("reason") or ""),
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
        command_text = _vps_deploy_command_text(normalized_command)
        with self._host_task_start_lock(hostname):
            vps = self._require_vps(hostname)
            self._apply_session_secrets_to_vps(token, vps)
            if _vps_deploy_requires_user_password(normalized_command) and not getattr(vps, "user_pw", None):
                raise ValueError(f"VPS user password missing for {hostname}.")
            self._raise_if_vps_task_active(vps, command_text)
            vps.command = normalized_command
            vps.command_text = command_text
            self.vpsmanager.update_vps(vps, debug=debug, extra_vars=extra_vars or None)
            task_log_name = vps._task_log_path(vps.command, COMMAND_VPS_UPDATE).name
        return {
            "command": normalized_command,
            "command_text": command_text,
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
                    try:
                        host_log = self._start_vps_deploy_host(
                            token,
                            hostname=hostname,
                            command=command,
                            debug=debug,
                            extra_vars=extra_vars,
                        )
                    except ValueError as exc:
                        if not self._is_vps_task_active_error(exc):
                            raise
                        host_log = self._skipped_vps_deploy_host_log(hostname, command, str(exc))
                        self._update_vps_deploy_entry(entry_id, host_logs={hostname: host_log})
                        continue
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
                try:
                    host_logs[hostname] = self._start_vps_deploy_host(
                        token,
                        hostname=hostname,
                        command=normalized_command,
                        debug=debug,
                        extra_vars=base_extra_vars or None,
                    )
                except ValueError as exc:
                    if "VPS user password missing" in str(exc):
                        raise ValueError(f"VPS sudo password missing for {hostname}. Validate it before starting {_vps_deploy_command_text(normalized_command)}.") from exc
                    if not self._is_vps_task_active_error(exc):
                        raise
                    host_logs[hostname] = self._skipped_vps_deploy_host_log(hostname, normalized_command, str(exc))

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
            "started_count": len([item for item in host_logs.values() if str(item.get("status") or "") != "skipped"]),
            "skipped_hosts": [
                {"hostname": host, "reason": str(payload.get("reason") or "Already running on this host.")}
                for host, payload in host_logs.items()
                if str(payload.get("status") or "") == "skipped"
            ],
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
            try:
                host_log = self._start_vps_deploy_host(
                    token,
                    hostname=clean_hostname,
                    command=normalized_command,
                    debug=debug,
                    extra_vars=base_extra_vars or None,
                )
            except ValueError as exc:
                if not self._is_vps_task_active_error(exc):
                    raise
                host_log = self._skipped_vps_deploy_host_log(clean_hostname, normalized_command, str(exc))
                entry = self._update_vps_deploy_entry(target_entry_id, host_logs={clean_hostname: host_log})
                return {
                    "command": normalized_command,
                    "command_text": _vps_deploy_command_text(normalized_command),
                    "mode": normalized_mode,
                    "hostnames": unique_hosts,
                    "count": len(unique_hosts),
                    "hostname": clean_hostname,
                    "started": False,
                    "queued": False,
                    "skipped": True,
                    "skip_reason": str(exc),
                    "entry_id": target_entry_id,
                    "entry": entry,
                }
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
                    "blocked": _truthy(instance.get("blocked", False)),
                    "blocked_reason": str(instance.get("blocked_reason") or ""),
                    "cluster_gate": str(instance.get("cluster_gate") or ""),
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
        with self._host_task_start_lock(hostname):
            vps = self._require_vps(hostname)
            self._apply_session_secrets_to_vps(token, vps)
            if command == COMMAND_VPS_MIGRATE_SYSTEMD and not getattr(vps, "user_pw", None):
                raise ValueError(f"VPS user password missing for {hostname}. It is required for sudo/become during systemd migration.")
            command_extra_vars = dict(extra_vars or {})
            if command in {COMMAND_VPS_UPDATE_PBGUI, COMMAND_VPS_UPDATE_PB}:
                monitor_state = self._get_monitor_state()
                host_state = self._get_host_telemetry(monitor_state, hostname)
                self._sync_vps_config_from_host_meta(vps, host_state)
                host_meta = self._host_meta(host_state)
                pending_optional = self._load_vps_optional_config_pending(vps)
                if host_meta.get("coindata_configured") is False and "coinmarketcap_api_key" not in command_extra_vars and "coinmarketcap_api_key" not in pending_optional:
                    command_extra_vars["coinmarketcap_api_key"] = ""
            self._raise_if_vps_task_active(vps, command_text)
            vps.command = command
            vps.command_text = command_text
            self.vpsmanager.update_vps(vps, debug=debug, extra_vars=command_extra_vars or None)

    def _start_vps_optional_config_apply(self, token: str, vps: VPS, *, apply_optional_config: bool, apply_firewall: bool, apply_swap: bool = False) -> dict[str, Any]:
        self._apply_session_secrets_to_vps(token, vps)
        vps.command = COMMAND_VPS_APPLY_CONFIG
        vps.command_text = "Apply VPS Config"
        extra_vars = {
            "apply_optional_config": bool(apply_optional_config),
            "apply_firewall": bool(apply_firewall),
            "apply_swap": bool(apply_swap),
        }
        if apply_optional_config:
            extra_vars["coinmarketcap_api_key"] = str(getattr(vps, "coinmarketcap_api_key", "") or "").strip()
        try:
            self.vpsmanager.update_vps(vps, debug=False, extra_vars=extra_vars)
        except Exception as exc:
            _log(SERVICE, f"failed to start VPS config apply for {vps.hostname}: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
            return {
                "started": False,
                "command": COMMAND_VPS_APPLY_CONFIG,
                "command_text": "Apply VPS Config",
                "error": str(exc),
            }
        task_log_name = ""
        try:
            task_log_name = vps._task_log_path(vps.command, COMMAND_VPS_UPDATE).name
        except Exception:
            task_log_name = ""
        return {
            "started": True,
            "command": COMMAND_VPS_APPLY_CONFIG,
            "command_text": "Apply VPS Config",
            "run_id": str(getattr(vps, "command_run_id", "") or ""),
            "filename": task_log_name,
        }

    def delete_vps(self, hostname: str) -> None:
        vps = self._require_vps(hostname)
        vps.delete()
        self.vpsmanager.vpss = [item for item in self.vpsmanager.vpss if item.hostname != hostname]
        self._set_vps_monitor_enabled(hostname, enabled=False)

    def read_vps_settings(self, token: str, hostname: str, form: dict[str, Any] | None = None, progress: Callable[[str, str, str], None] | None = None) -> dict[str, Any]:
        def emit(step: str, label: str, status: str = "running") -> None:
            if progress:
                progress(step, label, status)

        emit("start", "Preparing settings refresh")
        vps = self._require_vps(hostname)
        form = form or {}
        emit("password", "Checking VPS password")
        self._store_session_secrets(token, hostname, form)
        vps.user_pw = self._require_user_password(token, hostname)
        emit("ssh", "Connecting to VPS")
        if not vps.can_login_ssh():
            raise ValueError("Cannot login via SSH. Please check username and password.")
        emit("remote_config", "Reading remote pbgui.ini")
        info = vps.fetch_vps_info()
        vps.coinmarketcap_api_key = info["coinmarketcap"]
        vps.swap = info.get("swap", "0") if info.get("swap") in SWAP_OPTIONS else "0"
        if info.get("firewall_ssh_port") is not None:
            vps.firewall_ssh_port = _safe_int(info.get("firewall_ssh_port"), 22)
        emit("firewall", "Reading UFW firewall settings")
        vps.firewall, vps.firewall_ssh_ips = vps.fetch_ufw_settings()
        emit("save", "Saving refreshed VPS settings")
        vps.save()
        vps.write_vps_firewall_info()
        self._clear_vps_optional_config_pending(vps)
        emit("done", "VPS settings refreshed", "done")
        return self._build_vps_config(token, vps)

    def preview_vps_systemd_migration(self, token: str, hostname: str, form: dict[str, Any]) -> dict[str, Any]:
        import paramiko

        vps = self._require_vps(hostname)
        hostname = str(vps.hostname or hostname or "").strip()
        self._store_session_secrets(token, hostname, form)
        user_pw = str(form.get("user_pw") or self._session_secret_value(token, hostname, "user_pw") or "")
        raw_install_dir = str(form.get("install_dir") or "").strip()
        install_dir = _normalize_vps_install_dir(raw_install_dir, vps.user) if raw_install_dir else _install_dir_from_remote_pbgui_dir(vps.remote_pbgui_dir, vps.user)
        pbgui_dir = f"{install_dir.rstrip('/')}/pbgui"
        python_bin = f"{install_dir.rstrip('/')}/venv_pbgui/bin/python"

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            try:
                ssh.connect(
                    hostname=vps.ip,
                    username=vps.user,
                    timeout=8,
                    banner_timeout=8,
                    auth_timeout=8,
                    allow_agent=True,
                    look_for_keys=True,
                )
            except Exception:
                if not user_pw:
                    raise ValueError("VPS user password expired or missing. Please enter it again.")
                ssh.connect(
                    hostname=vps.ip,
                    username=vps.user,
                    password=user_pw,
                    timeout=8,
                    banner_timeout=8,
                    auth_timeout=8,
                    allow_agent=False,
                    look_for_keys=False,
                )

            script = self._vps_systemd_migration_preview_script(pbgui_dir, python_bin)
            stdin, stdout, stderr = ssh.exec_command(script, timeout=20)
            del stdin
            out = stdout.read().decode("utf-8", errors="replace")
            err = stderr.read().decode("utf-8", errors="replace")
            rc = stdout.channel.recv_exit_status()
            if rc != 0:
                raise ValueError((err or out or "Systemd migration preview failed.").strip())
        finally:
            try:
                ssh.close()
            except Exception:
                pass

        parsed = self._parse_vps_systemd_migration_preview(out)
        values = parsed.get("values") or {}
        units = parsed.get("units") or []
        cron_lines = parsed.get("cron") or []
        processes = parsed.get("processes") or []

        pbgui_exists = values.get("pbgui_dir_exists") == "yes"
        python_exists = values.get("python_exists") == "yes"
        systemctl_exists = values.get("systemctl_exists") == "yes"
        user_manager_ok = values.get("systemd_user_manager") == "yes"
        start_sh_exists = values.get("start_sh_exists") == "yes"
        required_units = self._systemd_migration_required_units(units, values)
        units_missing = [item for item in required_units if item.get("exists") != "yes"]
        units_not_enabled = [item for item in required_units if item.get("enabled") != "enabled"]
        units_inactive = [item for item in required_units if item.get("active") != "active"]
        units_ready = bool(required_units) and not units_missing and not units_not_enabled and not units_inactive
        blockers: list[str] = []
        if not pbgui_exists:
            blockers.append(f"PBGui directory not found: {pbgui_dir}")
        if not python_exists:
            blockers.append(f"PBGui virtualenv Python not found: {python_bin}")
        if not systemctl_exists:
            blockers.append("systemctl is not available on the VPS.")

        warnings: list[str] = []
        if not user_manager_ok:
            warnings.append("The systemd user manager is not active yet. Migration will enable linger and start it.")

        migration_complete = bool(pbgui_exists and python_exists and systemctl_exists and user_manager_ok and units_ready and not processes and not cron_lines and not start_sh_exists)
        required_unit_names = [item.get("unit") or "" for item in required_units if item.get("unit")]
        actions = [
            {"label": "Copy/update systemd setup helper", "needed": not migration_complete, "detail": f"{pbgui_dir}/setup/setup_systemd.sh"},
            {"label": "Install/enable/start required systemd units", "needed": bool(units_missing or units_not_enabled or units_inactive), "detail": ", ".join(required_unit_names) or "No required units"},
            {"label": "Stop legacy PBGui processes", "needed": bool(processes), "detail": f"{len(processes)} matching process(es)" if processes else "No matching legacy processes found"},
            {"label": "Remove legacy pbgui crontab", "needed": bool(cron_lines), "detail": f"{len(cron_lines)} matching crontab line(s)" if cron_lines else "No matching crontab lines found"},
            {"label": "Delete legacy start.sh", "needed": bool(start_sh_exists), "detail": f"{pbgui_dir}/start.sh"},
        ]

        checks = [
            {"label": "PBGui directory", "ok": pbgui_exists, "detail": pbgui_dir},
            {"label": "PBGui virtualenv Python", "ok": python_exists, "detail": python_bin},
            {"label": "systemctl available", "ok": systemctl_exists, "detail": values.get("systemctl_path") or "not found"},
            {"label": "systemd user manager", "ok": user_manager_ok, "detail": values.get("systemd_user_manager_detail") or "not active"},
        ]

        return {
            "hostname": hostname,
            "install_dir": install_dir,
            "pbgui_dir": pbgui_dir,
            "python_bin": python_bin,
            "checks": checks,
            "actions": actions,
            "warnings": warnings,
            "blockers": blockers,
            "units": units,
            "legacy_cron_lines": cron_lines,
            "legacy_processes": processes,
            "migration_complete": migration_complete,
            "migration_needed": not migration_complete,
            "can_migrate": not blockers and not migration_complete,
        }

    def _vps_systemd_migration_preview_script(self, pbgui_dir: str, python_bin: str) -> str:
        units = " ".join(shlex.quote(unit) for unit in VPS_SYSTEMD_MIGRATION_UNITS)
        return f"""#!/usr/bin/env bash
set +e
pbgui_dir={shlex.quote(pbgui_dir)}
python_bin={shlex.quote(python_bin)}
units={shlex.quote(units)}
uid=$(id -u)
systemctl_path=$(command -v systemctl || true)
printf 'KV\tpbgui_dir_exists\t%s\n' "$([ -d "$pbgui_dir" ] && printf yes || printf no)"
printf 'KV\tpython_exists\t%s\n' "$([ -x "$python_bin" ] && printf yes || printf no)"
printf 'KV\tstart_sh_exists\t%s\n' "$([ -e "$pbgui_dir/start.sh" ] && printf yes || printf no)"
printf 'KV\tsystemctl_exists\t%s\n' "$([ -n "$systemctl_path" ] && printf yes || printf no)"
printf 'KV\tsystemctl_path\t%s\n' "$systemctl_path"
PBGUI_CONFIG_PATH="$pbgui_dir/pbgui.ini" PBGUI_DIR="$pbgui_dir" python3 - <<'PY' 2>/dev/null || {{ printf 'KV\tpbrun_configured\tno\n'; printf 'KV\tpbdata_configured\tno\n'; printf 'KV\tcoindata_configured\tno\n'; }}
import ast
import configparser
import json
import os
import platform
from pathlib import Path

config = configparser.ConfigParser()
config.read(os.environ.get('PBGUI_CONFIG_PATH') or '')

def configured(value):
    normalized = str(value or '').strip()
    lowered = normalized.lower()
    if not normalized or lowered in {{'none', 'null', 'false', '<api_key>'}}:
        return False
    return not normalized.startswith('<')

def parsed_list(raw):
    text = str(raw or '').strip()
    if not text:
        return []
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, (list, tuple, set)):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except Exception:
        pass
    return [part.strip() for part in text.split(',') if part.strip()]

def pbrun_required():
    pbgui_dir = Path(os.environ.get('PBGUI_DIR') or '')
    pbname = config.get('main', 'pbname', fallback=platform.node()).strip() or platform.node()
    run_root = pbgui_dir / 'data' / 'run_v7'
    if not pbname or not run_root.is_dir():
        return False
    for cfg_path in run_root.glob('*/config.json'):
        try:
            payload = json.loads(cfg_path.read_text(encoding='utf-8'))
        except Exception:
            continue
        pbgui = payload.get('pbgui') if isinstance(payload, dict) else None
        enabled_on = str((pbgui or {{}}).get('enabled_on') or '').strip()
        if enabled_on and enabled_on != 'disabled' and enabled_on == pbname:
            return True
    return False

def pbdata_required():
    if not config.has_section('pbdata'):
        return False
    fetch_users = parsed_list(config.get('pbdata', 'fetch_users', fallback=''))
    trades_users = parsed_list(config.get('pbdata', 'trades_users', fallback=''))
    return bool(fetch_users or trades_users)

print('KV\tpbrun_configured\t' + ('yes' if pbrun_required() else 'no'))
print('KV\tpbdata_configured\t' + ('yes' if pbdata_required() else 'no'))
print('KV\tcoindata_configured\t' + ('yes' if configured(config.get('coinmarketcap', 'api_key', fallback='')) else 'no'))
PY
if [ -n "$systemctl_path" ] && env XDG_RUNTIME_DIR="${{XDG_RUNTIME_DIR:-/run/user/$uid}}" systemctl --user show-environment >/dev/null 2>&1; then
  printf 'KV\tsystemd_user_manager\tyes\n'
  printf 'KV\tsystemd_user_manager_detail\tactive\n'
else
  printf 'KV\tsystemd_user_manager\tno\n'
  printf 'KV\tsystemd_user_manager_detail\tnot active\n'
fi
printf 'SECTION\tunits\tBEGIN\n'
unit_dir="$HOME/.config/systemd/user"
for unit in $units; do
  exists="$([ -f "$unit_dir/$unit" ] && printf yes || printf no)"
  if [ -n "$systemctl_path" ]; then
    active=$(env XDG_RUNTIME_DIR="${{XDG_RUNTIME_DIR:-/run/user/$uid}}" systemctl --user is-active "$unit" 2>/dev/null || true)
    enabled=$(env XDG_RUNTIME_DIR="${{XDG_RUNTIME_DIR:-/run/user/$uid}}" systemctl --user is-enabled "$unit" 2>/dev/null || true)
  else
    active="unknown"
    enabled="unknown"
  fi
  printf '%s\t%s\t%s\t%s\n' "$unit" "$exists" "${{enabled:-unknown}}" "${{active:-unknown}}"
done
printf 'SECTION\tunits\tEND\n'
printf 'SECTION\tcron\tBEGIN\n'
crontab -l 2>/dev/null | awk -v start="$pbgui_dir/start.sh" 'index($0, start) {{ print }}'
printf 'SECTION\tcron\tEND\n'
printf 'SECTION\tprocesses\tBEGIN\n'
PBGUI_MIGRATION_DIR="$pbgui_dir" python3 - <<'PY' 2>/dev/null || true
import os
from pathlib import Path
target_dir = os.path.realpath(os.environ['PBGUI_MIGRATION_DIR'])
target_prefix = target_dir + os.sep
scripts = ('PBCluster.py', 'PBRun.py', 'PBCoinData.py', 'starter.py')
unit_by_script = {{
    'PBCluster.py': 'pbgui-pbcluster.service',
    'PBRun.py': 'pbgui-pbrun.service',
    'PBCoinData.py': 'pbgui-pbcoindata.service',
}}

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
        cgroup = Path(f'/proc/{{pid}}/cgroup').read_text(encoding='utf-8', errors='replace')
    except Exception:
        return False
    return unit in cgroup

def ignored_process_ids():
    ignored = {{str(os.getpid())}}
    pid = os.getpid()
    while True:
        try:
            stat = Path(f'/proc/{{pid}}/stat').read_text(encoding='utf-8', errors='replace')
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

ignored_pids = ignored_process_ids()

for entry in Path('/proc').iterdir():
    if not entry.name.isdigit():
        continue
    if entry.name in ignored_pids:
        continue
    try:
        raw = (entry / 'cmdline').read_bytes()
    except Exception:
        continue
    cmd = raw.replace(b'\\0', b' ').decode('utf-8', errors='replace').strip()
    try:
        cwd = os.path.realpath(os.readlink(entry / 'cwd'))
    except Exception:
        cwd = ''
    script = matching_script(cmd)
    if cmd and script and (target_prefix in cmd or cwd == target_dir) and not is_systemd_managed(entry.name, script):
        print(f"{{entry.name}} {{cmd}}")
PY
printf 'SECTION\tprocesses\tEND\n'
"""

    def _parse_vps_systemd_migration_preview(self, output: str) -> dict[str, Any]:
        values: dict[str, str] = {}
        sections: dict[str, list[str]] = {"units": [], "cron": [], "processes": []}
        current = ""
        for raw_line in str(output or "").splitlines():
            line = raw_line.rstrip("\n")
            if line.startswith("KV\t"):
                parts = line.split("\t", 2)
                if len(parts) == 3:
                    values[parts[1]] = parts[2]
                continue
            if line.startswith("SECTION\t"):
                parts = line.split("\t")
                if len(parts) >= 3 and parts[2] == "BEGIN":
                    current = parts[1]
                    sections.setdefault(current, [])
                elif len(parts) >= 3 and parts[2] == "END":
                    current = ""
                continue
            if current:
                sections.setdefault(current, []).append(line)

        units: list[dict[str, str]] = []
        for line in sections.get("units") or []:
            parts = line.split("\t")
            if len(parts) >= 4:
                units.append({"unit": parts[0], "exists": parts[1], "enabled": parts[2], "active": parts[3]})
        return {
            "values": values,
            "units": units,
            "cron": sections.get("cron") or [],
            "processes": sections.get("processes") or [],
        }

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

    def _exec_import_ssh_command(
        self,
        ssh: Any,
        command: str,
        *,
        sudo_password: str | None = None,
        timeout: int = 10,
    ) -> tuple[int, str, str]:
        stdin, stdout, stderr = ssh.exec_command(command, timeout=timeout, get_pty=False)
        if sudo_password is not None:
            stdin.write(str(sudo_password or "") + "\n")
            stdin.flush()
        else:
            try:
                stdin.close()
            except Exception:
                pass
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        rc = int(stdout.channel.recv_exit_status())
        return rc, out, err

    def _test_import_key_login(self, *, ssh_host: str, user: str, timeout: int = 6) -> tuple[bool, str]:
        import paramiko

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        try:
            known_hosts = _user_known_hosts_path()
            if known_hosts.exists():
                ssh.load_host_keys(str(known_hosts))
        except Exception:
            pass
        ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
        try:
            ssh.connect(
                hostname=ssh_host,
                username=user,
                timeout=timeout,
                banner_timeout=timeout,
                auth_timeout=timeout,
                allow_agent=True,
                look_for_keys=True,
            )
            return True, "Key authentication succeeded."
        except Exception as exc:
            return False, str(exc) or "Key authentication failed."
        finally:
            try:
                ssh.close()
            except Exception:
                pass

    def _install_import_monitoring_key(self, *, ssh_host: str, user: str, user_pw: str) -> tuple[bool, str]:
        """Install this master's SSH public key so imported VPS monitoring can use key auth."""
        import paramiko

        try:
            public_key_path, public_key = _ensure_import_public_key()
        except Exception as exc:
            return False, str(exc) or "Could not prepare local SSH public key."

        try:
            remote_key = _fetch_remote_host_key(ssh_host, 22, timeout=8)
            host_key_status = _known_host_key_status(ssh_host, 22, remote_key)
            if host_key_status == "mismatch":
                return False, "SSH host key mismatch. Fix known_hosts intentionally before importing."
            if host_key_status != "known":
                _remember_known_host_key(ssh_host, 22, remote_key)
        except Exception as exc:
            return False, f"Cannot verify SSH host key for {ssh_host}: {exc}"

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        try:
            known_hosts = _user_known_hosts_path()
            if known_hosts.exists():
                ssh.load_host_keys(str(known_hosts))
        except Exception:
            pass
        ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
        try:
            ssh.connect(
                hostname=ssh_host,
                username=user,
                password=user_pw,
                timeout=8,
                banner_timeout=8,
                auth_timeout=8,
                allow_agent=False,
                look_for_keys=False,
            )
            key_literal = shlex.quote(public_key)
            install_cmd = f"""set -eu
umask 077
mkdir -p "$HOME/.ssh"
touch "$HOME/.ssh/authorized_keys"
chmod 700 "$HOME/.ssh"
chmod 600 "$HOME/.ssh/authorized_keys"
key={key_literal}
if grep -qxF "$key" "$HOME/.ssh/authorized_keys"; then
  printf 'SSH key already present in authorized_keys.\n'
else
  printf '%s\n' "$key" >> "$HOME/.ssh/authorized_keys"
  printf 'SSH key added to authorized_keys.\n'
fi"""
            rc, out, err = self._exec_import_ssh_command(ssh, install_cmd, timeout=10)
            if rc != 0:
                output = ((err or "") + (out or "")).strip()
                return False, output or "Could not install SSH key on the VPS."
        except Exception as exc:
            return False, str(exc) or "Could not install SSH key on the VPS."
        finally:
            try:
                ssh.close()
            except Exception:
                pass

        key_auth_ok, key_auth_detail = self._test_import_key_login(ssh_host=ssh_host, user=user)
        if key_auth_ok:
            return True, f"Installed {public_key_path}; key authentication succeeded."
        return False, key_auth_detail or "SSH key was installed, but key authentication still failed."

    def probe_existing_vps_import(self, form: dict[str, Any]) -> dict[str, Any]:
        import paramiko

        hostname = _validate_import_hostname(form.get("hostname"))
        ip = str(form.get("ip") or "").strip()
        if not _valid_ipv4(ip):
            raise ValueError("IP address is not valid.")
        user = str(form.get("user") or "").strip()
        if not user:
            raise ValueError("VPS user is required.")
        user_pw = str(form.get("user_pw") or "")
        raw_install_dir = str(form.get("install_dir") or "").strip() or f"/home/{user}/software"
        install_dir = _normalize_vps_install_dir(raw_install_dir, user)
        pbgui_dir = f"{install_dir.rstrip('/')}/pbgui"
        pbgui_ini = f"{pbgui_dir}/pbgui.ini"
        python_bin = f"{install_dir.rstrip('/')}/venv_pbgui/bin/python"
        default_pb7_dir = f"{install_dir.rstrip('/')}/pb7"
        default_pb7_venv = f"{install_dir.rstrip('/')}/venv_pb7/bin/python"
        accept_unknown_host = bool(form.get("accept_unknown_host"))
        accepted_fingerprint = str(form.get("accepted_host_key_fingerprint") or "").strip()

        result: dict[str, Any] = {
            "hostname": hostname,
            "ip": ip,
            "user": user,
            "install_dir": install_dir,
            "pbgui_dir": pbgui_dir,
            "checks": [],
            "warnings": [],
            "blockers": [],
            "host_key": {},
            "local_hosts_ok": False,
            "local_hosts_update_required": False,
            "needs_host_key_confirmation": False,
            "detected": {
                "remote_pbgui_dir": pbgui_dir,
                "pbgui_dir": pbgui_dir,
                "pbgui_ini": pbgui_ini,
                "python_bin": python_bin,
                "pb7_dir": default_pb7_dir,
                "pb7_venv": default_pb7_venv,
                "swap": "0",
                "coinmarketcap_api_key": "",
                "firewall": False,
                "firewall_ssh_port": 22,
                "firewall_ssh_ips": "",
                "key_auth_ok": False,
                "systemd_units": [],
                "legacy_cron_lines": [],
                "legacy_processes": [],
                "legacy_start_sh_exists": False,
            },
            "can_save": False,
        }

        def add_check(label: str, ok: bool, detail: str = "") -> None:
            result["checks"].append({"label": label, "ok": bool(ok), "detail": str(detail or "")})

        def add_warning(message: str) -> None:
            text = str(message or "").strip()
            if text and text not in result["warnings"]:
                result["warnings"].append(text)

        def add_blocker(message: str) -> None:
            text = str(message or "").strip()
            if text and text not in result["blockers"]:
                result["blockers"].append(text)

        install_dir_hints: list[str] = []

        def set_import_install_dir(next_install_dir: str) -> None:
            nonlocal install_dir, pbgui_dir, pbgui_ini, python_bin, default_pb7_dir, default_pb7_venv
            install_dir = _normalize_vps_install_dir(next_install_dir, user)
            pbgui_dir = f"{install_dir.rstrip('/')}/pbgui"
            pbgui_ini = f"{pbgui_dir}/pbgui.ini"
            python_bin = f"{install_dir.rstrip('/')}/venv_pbgui/bin/python"
            default_pb7_dir = f"{install_dir.rstrip('/')}/pb7"
            default_pb7_venv = f"{install_dir.rstrip('/')}/venv_pb7/bin/python"
            result["install_dir"] = install_dir
            result["pbgui_dir"] = pbgui_dir
            result["detected"].update({
                "remote_pbgui_dir": pbgui_dir,
                "pbgui_dir": pbgui_dir,
                "pbgui_ini": pbgui_ini,
                "python_bin": python_bin,
                "pb7_dir": default_pb7_dir,
                "pb7_venv": default_pb7_venv,
            })

        def install_dir_candidates() -> list[str]:
            raw_path = str(raw_install_dir or install_dir).strip().rstrip("/") or f"/home/{user}/software"
            paths = list(install_dir_hints) + [raw_path]
            try:
                raw_posix = PurePosixPath(raw_path)
                if raw_posix.name == "pbgui":
                    paths.append(str(raw_posix.parent))
                paths.append(str(raw_posix / "software"))
                if str(raw_posix.parent) and raw_posix.parent != raw_posix:
                    paths.append(str(raw_posix.parent / "software"))
            except Exception:
                pass
            paths.extend([f"/home/{user}/software", f"/home/{user}"])
            out: list[str] = []
            for path in paths:
                try:
                    candidate = _normalize_vps_install_dir(path, user)
                except ValueError:
                    continue
                if candidate not in out:
                    out.append(candidate)
            return out

        def expand_remote_user_path(path: str) -> str:
            text = str(path or "").strip()
            if text.startswith("~/"):
                return f"/home/{user}/{text[2:]}"
            if text.startswith("$HOME/"):
                return f"/home/{user}/{text[6:]}"
            if text.startswith("${HOME}/"):
                return f"/home/{user}/{text[8:]}"
            return text

        def add_install_dir_hint(path: PurePosixPath | str) -> None:
            try:
                candidate = _normalize_vps_install_dir(expand_remote_user_path(str(path)), user)
            except ValueError:
                return
            if candidate not in install_dir_hints:
                install_dir_hints.append(candidate)

        def add_process_install_dir_candidates(output: str) -> None:
            script_pattern = r"(?:PBRun|PBCluster|PBCoinData|starter)\.py"
            absolute_pattern = re.compile(rf"(/[^\s'\"]+/{script_pattern})")
            relative_name_pattern = re.compile(rf"(^|[\s'\"])(?:\./)?{script_pattern}($|[\s'\"])")
            relative_pbgui_pattern = re.compile(rf"(^|[\s'\"])pbgui/{script_pattern}($|[\s'\"])")

            for match in absolute_pattern.findall(str(output or "")):
                script_dir = PurePosixPath(match).parent
                candidates = [script_dir.parent] if script_dir.name == "pbgui" else [script_dir, script_dir.parent]
                for path in candidates:
                    add_install_dir_hint(path)

            for line in str(output or "").splitlines():
                parts = line.split("\t", 2)
                if len(parts) < 3:
                    continue
                cwd = parts[1].strip().rstrip("/")
                if not cwd:
                    continue
                args = parts[2]
                cwd_path = PurePosixPath(cwd)
                if cwd_path.name == "pbgui":
                    add_install_dir_hint(cwd_path.parent)
                elif relative_pbgui_pattern.search(args) or relative_name_pattern.search(args):
                    add_install_dir_hint(cwd_path)

        def add_cron_install_dir_candidates(lines: list[str]) -> None:
            pbgui_path_pattern = re.compile(r"((?:/|~/|\$HOME/|\$\{HOME\}/)[^\s'\"]*/pbgui)(?:/start\.sh)?")
            for line in lines:
                if "pbgui" not in line or "start.sh" not in line:
                    continue
                for match in pbgui_path_pattern.findall(line):
                    add_install_dir_hint(PurePosixPath(match).parent)

        def detect_install_dir(sftp: Any) -> str:
            fallback = ""
            for candidate in install_dir_candidates():
                base = candidate.rstrip("/")
                candidate_ini = f"{base}/pbgui/pbgui.ini"
                candidate_python = f"{base}/venv_pbgui/bin/python"
                has_ini = _sftp_path_exists(sftp, candidate_ini)
                has_python = _sftp_path_exists(sftp, candidate_python)
                if has_ini and has_python:
                    return candidate
                if has_ini and not fallback:
                    fallback = candidate
            return fallback

        if hostname in self.vpsmanager.list():
            add_check("VPS Manager record", False, "Hostname already exists.")
            add_blocker("Hostname already exists in VPS Manager.")
            return result
        add_check("VPS Manager record", True, "Hostname is not saved yet.")

        hosts_status = _hosts_entry_status(hostname, ip)
        ssh_target = hostname
        if hosts_status.get("ok"):
            add_check("/etc/hosts", True, f"{hostname} resolves to {ip}.")
            result["local_hosts_ok"] = True
        elif hosts_status.get("has_hostname"):
            current_ip = str(hosts_status.get("current_ip") or "")
            add_check("/etc/hosts", False, f"Hostname maps to {current_ip}, expected {ip}.")
            add_warning(f"Local /etc/hosts maps {hostname} to {current_ip} instead of {ip}; saving the import will replace it with {ip}.")
            result["local_hosts_update_required"] = True
            ssh_target = ip
        else:
            add_check("/etc/hosts", False, "Hostname is missing locally.")
            add_warning(f"Local /etc/hosts is missing '{ip} {hostname}'; saving the import will add it after local sudo confirmation.")
            result["local_hosts_update_required"] = True
            ssh_target = ip

        try:
            remote_key = _fetch_remote_host_key(ssh_target, 22, timeout=8)
            fingerprint = _ssh_fingerprint_sha256(remote_key)
            status = _known_host_key_status(hostname, 22, remote_key)
            host_key = {
                "host": hostname,
                "ip": ip,
                "port": 22,
                "key_type": remote_key.get_name(),
                "fingerprint": fingerprint,
                "status": status,
                "known": status == "known",
                "mismatch": status == "mismatch",
            }
            result["host_key"] = host_key
        except Exception as exc:
            add_check("SSH host key", False, str(exc) or "Cannot read host key.")
            add_blocker(f"Cannot read SSH host key for {hostname}: {exc}")
            return result

        if result["host_key"].get("mismatch"):
            add_check("SSH host key", False, "Known host key mismatch.")
            add_blocker("SSH host key mismatch. Fix known_hosts intentionally before importing.")
            return result
        if result["host_key"].get("status") != "known":
            if not accept_unknown_host:
                result["needs_host_key_confirmation"] = True
                add_check("SSH host key", False, "Fingerprint confirmation required.")
                add_warning("Confirm the SSH host key fingerprint before continuing.")
                return result
            fingerprint = str(result["host_key"].get("fingerprint") or "")
            if not _ssh_fingerprints_match(accepted_fingerprint, fingerprint):
                result["needs_host_key_confirmation"] = True
                add_check("SSH host key", False, "Accepted fingerprint does not match.")
                add_blocker("Accepted SSH host key fingerprint does not match the presented key.")
                return result
            _remember_known_host_key(hostname, 22, remote_key)
            if ssh_target != hostname:
                _remember_known_host_key(ssh_target, 22, remote_key)
            result["host_key"]["status"] = "known"
            result["host_key"]["known"] = True
            add_check("SSH host key", True, f"Trusted {remote_key.get_name()} {fingerprint}.")
        else:
            if ssh_target != hostname:
                _remember_known_host_key(ssh_target, 22, remote_key)
            add_check("SSH host key", True, "Known host key matches.")

        if not user_pw:
            add_check("SSH password login", False, "VPS user password is required.")
            add_blocker("VPS user password is required.")
            return result

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        try:
            known_hosts = _user_known_hosts_path()
            if known_hosts.exists():
                ssh.load_host_keys(str(known_hosts))
        except Exception:
            pass
        ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
        try:
            ssh.connect(
                hostname=ssh_target,
                username=user,
                password=user_pw,
                timeout=8,
                banner_timeout=8,
                auth_timeout=8,
                allow_agent=False,
                look_for_keys=False,
            )
            add_check("SSH password login", True, f"Connected as {user}.")
        except paramiko.AuthenticationException:
            add_check("SSH password login", False, "Authentication failed.")
            add_blocker("Cannot connect via SSH with the supplied VPS user password.")
            return result
        except Exception as exc:
            add_check("SSH password login", False, str(exc) or "SSH connection failed.")
            add_blocker(f"Cannot connect via SSH: {exc}")
            return result

        try:
            sudo_rc, sudo_out, sudo_err = self._exec_import_ssh_command(
                ssh,
                "sudo -k -S -p '' -v",
                sudo_password=user_pw,
                timeout=10,
            )
            sudo_combined = "\n".join([sudo_out, sudo_err]).strip()
            sudo_lower = sudo_combined.lower()
            wrong_sudo = any(marker in sudo_lower for marker in (
                "incorrect password",
                "sorry, try again",
                "no password was provided",
                "a password is required",
                "1 incorrect password attempt",
            ))
            if sudo_rc != 0 or wrong_sudo:
                detail = sudo_combined.splitlines()[0].strip() if sudo_combined else "sudo validation failed"
                add_check("sudo access", False, detail)
                add_blocker("The VPS user must have sudo access for future VPS Manager actions.")
                return result
            add_check("sudo access", True, "sudo -v succeeded.")

            swap_rc, swap_out, _swap_err = self._exec_import_ssh_command(
                ssh,
                "swapon --show --noheadings --raw | awk '$1==\"/swapfile\" {print $3; found=1; exit} END {if (!found) print \"0\"}'",
                timeout=8,
            )
            swap_value = swap_out.strip().splitlines()[0].strip() if swap_rc == 0 and swap_out.strip() else "0"
            if swap_value not in SWAP_OPTIONS:
                add_warning(f"Detected swap size '{swap_value}' is not a supported VPS Manager option; saved as 0.")
                swap_value = "0"
            result["detected"]["swap"] = swap_value
            add_check("Swap", True, swap_value)

            ufw_rc, ufw_out, ufw_err = self._exec_import_ssh_command(
                ssh,
                "sudo -S -p '' ufw status",
                sudo_password=user_pw,
                timeout=10,
            )
            if ufw_rc == 0:
                firewall, firewall_ips = _parse_ufw_status(ufw_out)
                result["detected"]["firewall"] = firewall
                result["detected"]["firewall_ssh_ips"] = firewall_ips
                add_check("UFW", True, "active" if firewall else "inactive")
            else:
                add_warning((ufw_err or ufw_out or "Could not read UFW status.").strip())

            proc_cmd = """python3 - <<'PY' 2>/dev/null || true
import os
from pathlib import Path

scripts = ('PBCluster.py', 'PBRun.py', 'PBCoinData.py', 'starter.py')
unit_by_script = {
    'PBCluster.py': 'pbgui-pbcluster.service',
    'PBRun.py': 'pbgui-pbrun.service',
    'PBCoinData.py': 'pbgui-pbcoindata.service',
}

def matching_script(cmd):
    for script in scripts:
        if script in cmd:
            return script
    return ''

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
    if not entry.name.isdigit():
        continue
    try:
        raw = (entry / 'cmdline').read_bytes()
    except Exception:
        continue
    cmd = raw.replace(b'\\0', b' ').decode('utf-8', errors='replace').strip()
    script = matching_script(cmd)
    if not cmd or not script:
        continue
    try:
        cwd = os.path.realpath(os.readlink(entry / 'cwd'))
    except Exception:
        cwd = ''
    manager = 'systemd' if is_systemd_managed(entry.name, script) else 'legacy'
    print(f"{entry.name}\t{cwd}\t{manager}\t{cmd}")
PY"""
            _proc_rc, proc_out, _proc_err = self._exec_import_ssh_command(ssh, proc_cmd, timeout=8)
            add_process_install_dir_candidates(proc_out)

            cron_rc, cron_out, _cron_err = self._exec_import_ssh_command(ssh, "crontab -l 2>/dev/null || true", timeout=8)
            cron_lines = []
            if cron_rc == 0:
                cron_lines = [
                    line
                    for line in cron_out.splitlines()
                    if "pbgui" in line and "start.sh" in line
                ]
                add_cron_install_dir_candidates(cron_lines)

            sftp = ssh.open_sftp()
            try:
                detected_install_dir = detect_install_dir(sftp)
            finally:
                try:
                    sftp.close()
                except Exception:
                    pass
            if detected_install_dir and detected_install_dir != install_dir:
                original_install_dir = install_dir
                set_import_install_dir(detected_install_dir)
                add_check("Install path", True, f"Detected {install_dir}.")
                add_warning(f"Install path '{original_install_dir}' did not contain PBGui; using detected path '{install_dir}'.")

            units = " ".join(shlex.quote(unit) for unit in VPS_SYSTEMD_MIGRATION_UNITS)
            units_cmd = f"""uid=$(id -u)
systemctl_path=$(command -v systemctl || true)
unit_dir=\"$HOME/.config/systemd/user\"
for unit in {units}; do
  exists=\"$([ -f \"$unit_dir/$unit\" ] && printf yes || printf no)\"
  if [ -n \"$systemctl_path\" ]; then
    active=$(env XDG_RUNTIME_DIR=\"${{XDG_RUNTIME_DIR:-/run/user/$uid}}\" systemctl --user is-active \"$unit\" 2>/dev/null || true)
    enabled=$(env XDG_RUNTIME_DIR=\"${{XDG_RUNTIME_DIR:-/run/user/$uid}}\" systemctl --user is-enabled \"$unit\" 2>/dev/null || true)
  else
    active=unknown
    enabled=unknown
  fi
  printf '%s\t%s\t%s\t%s\n' \"$unit\" \"$exists\" \"${{enabled:-unknown}}\" \"${{active:-unknown}}\"
done"""
            _units_rc, units_out, _units_err = self._exec_import_ssh_command(ssh, units_cmd, timeout=12)
            systemd_units = _parse_import_systemd_units(units_out)
            result["detected"]["systemd_units"] = systemd_units
            active_units = [unit for unit in systemd_units if unit.get("active") == "active"]
            add_check("systemd user units", bool(active_units), f"{len(active_units)} active unit(s).")

            if cron_rc == 0:
                result["detected"]["legacy_cron_lines"] = cron_lines
                if cron_lines:
                    add_warning(f"Found {len(cron_lines)} legacy pbgui crontab line(s). Use systemd migration after import.")

            legacy_processes = [
                line.strip()
                for line in proc_out.splitlines()
                if _import_process_line_is_legacy(line, pbgui_dir)
            ]
            result["detected"]["legacy_processes"] = legacy_processes
            if legacy_processes:
                add_warning(f"Found {len(legacy_processes)} running legacy PBGui process(es). Use systemd migration after import.")

            sftp = ssh.open_sftp()
            try:
                pbgui_exists = _sftp_path_exists(sftp, pbgui_dir)
                python_exists = _sftp_path_exists(sftp, python_bin)
                pbgui_ini_exists = _sftp_path_exists(sftp, pbgui_ini)
                start_sh_exists = _sftp_path_exists(sftp, f"{pbgui_dir}/start.sh")
                result["detected"]["legacy_start_sh_exists"] = start_sh_exists
                add_check("PBGui directory", pbgui_exists, pbgui_dir)
                add_check("PBGui virtualenv Python", python_exists, python_bin)
                add_check("pbgui.ini", pbgui_ini_exists, pbgui_ini)
                if not pbgui_exists:
                    add_blocker(f"PBGui directory not found: {pbgui_dir}")
                if not python_exists:
                    add_blocker(f"PBGui virtualenv Python not found: {python_bin}")
                if not pbgui_ini_exists:
                    add_blocker(f"pbgui.ini not found: {pbgui_ini}")
                if start_sh_exists:
                    add_warning("Legacy start.sh exists. Use systemd migration after import.")

                if pbgui_ini_exists:
                    with sftp.open(pbgui_ini, "r") as handle:
                        ini_content = handle.read().decode("utf-8", errors="replace")
                    config = configparser.ConfigParser()
                    config.read_string(ini_content)
                    remote_pbname = config.get("main", "pbname", fallback="").strip()
                    pb7_dir = config.get("main", "pb7dir", fallback=default_pb7_dir).strip() or default_pb7_dir
                    pb7_venv = config.get("main", "pb7venv", fallback=default_pb7_venv).strip() or default_pb7_venv
                    cmc_key = config.get("coinmarketcap", "api_key", fallback="").strip()
                    if config.has_section("firewall"):
                        result["detected"]["firewall"] = _truthy(config.get("firewall", "enabled", fallback=""))
                        result["detected"]["firewall_ssh_port"] = _safe_int(config.get("firewall", "ssh_port", fallback="22"), 22)
                        result["detected"]["firewall_ssh_ips"] = config.get("firewall", "ssh_ips", fallback="").strip()
                    result["detected"]["pb7_dir"] = pb7_dir
                    result["detected"]["pb7_venv"] = pb7_venv
                    result["detected"]["coinmarketcap_api_key"] = cmc_key
                    if remote_pbname and remote_pbname != hostname:
                        add_warning(f"Remote pbgui.ini pbname is '{remote_pbname}', not '{hostname}'.")
                    if not cmc_key:
                        add_warning("Remote pbgui.ini has no CoinMarketCap API key.")
                    pb7_dir_exists = _sftp_path_exists(sftp, pb7_dir)
                    pb7_venv_exists = _sftp_path_exists(sftp, pb7_venv)
                    add_check("PB7 directory", pb7_dir_exists, pb7_dir)
                    add_check("PB7 virtualenv Python", pb7_venv_exists, pb7_venv)
                    if not pb7_dir_exists:
                        add_warning(f"PB7 directory not found: {pb7_dir}")
                    if not pb7_venv_exists:
                        add_warning(f"PB7 virtualenv Python not found: {pb7_venv}")
            finally:
                try:
                    sftp.close()
                except Exception:
                    pass
        finally:
            try:
                ssh.close()
            except Exception:
                pass

        key_auth_ok, key_auth_detail = self._test_import_key_login(ssh_host=ssh_target, user=user)
        result["detected"]["key_auth_ok"] = key_auth_ok
        add_check("SSH key login for monitoring", key_auth_ok, key_auth_detail)
        if not key_auth_ok:
            add_warning(IMPORT_KEY_INSTALL_WARNING)

        result["can_save"] = not result["blockers"] and not result["needs_host_key_confirmation"]
        return result

    def resolve_existing_vps_import_host(self, hostname: str) -> dict[str, Any]:
        hostname = _validate_import_hostname(hostname)
        lookup = _hosts_entry_lookup(hostname)
        ip = str(lookup.get("ip") or "").strip()
        return {
            "hostname": hostname,
            "found": bool(lookup.get("found") and _valid_ipv4(ip)),
            "ip": ip if _valid_ipv4(ip) else "",
        }

    def save_existing_vps_import(self, token: str, form: dict[str, Any]) -> dict[str, Any]:
        probe = self.probe_existing_vps_import(form)
        if probe.get("needs_host_key_confirmation"):
            raise ValueError("Confirm the SSH host key fingerprint before saving this VPS.")
        blockers = [str(item) for item in (probe.get("blockers") or []) if str(item).strip()]
        if blockers:
            raise ValueError(blockers[0])

        hostname = str(probe.get("hostname") or "").strip()
        if not hostname:
            raise ValueError("Hostname is required.")
        if hostname in self.vpsmanager.list():
            raise ValueError("Hostname already exists.")

        if probe.get("local_hosts_update_required"):
            local_sudo_pw = str(form.get("local_sudo_pw") or "")
            if not local_sudo_pw:
                raise ValueError(f"Local sudo password is required to add '{probe.get('ip')} {hostname}' to /etc/hosts.")
            hosts_result = self.write_hosts_entry(str(probe.get("ip") or ""), hostname, local_sudo_pw)
            if not hosts_result.get("ok"):
                raise ValueError(str(hosts_result.get("error") or "Failed to update local /etc/hosts."))

        detected = probe.get("detected") or {}
        user_pw = str(form.get("user_pw") or "")
        user = str(probe.get("user") or "").strip()
        if not detected.get("key_auth_ok") and user and user_pw:
            key_auth_ok, key_auth_detail = self._install_import_monitoring_key(ssh_host=hostname, user=user, user_pw=user_pw)
            detected["key_auth_ok"] = key_auth_ok
            _set_import_key_check(probe, key_auth_ok, key_auth_detail)
            warnings = probe.get("warnings")
            if isinstance(warnings, list):
                if key_auth_ok:
                    probe["warnings"] = [warning for warning in warnings if warning != IMPORT_KEY_INSTALL_WARNING]
                else:
                    install_warning = f"Automatic SSH key installation failed: {key_auth_detail}"
                    if install_warning not in warnings:
                        warnings.append(install_warning)
        self._store_session_secrets(token, hostname, {"user_pw": user_pw})

        vps = VPS()
        vps.hostname = hostname
        vps.ip = str(probe.get("ip") or "").strip()
        vps.user = user
        vps.remote_pbgui_dir = str(detected.get("remote_pbgui_dir") or detected.get("pbgui_dir") or "").strip()
        vps.swap = str(detected.get("swap") or "0") if str(detected.get("swap") or "0") in SWAP_OPTIONS else "0"
        vps.coinmarketcap_api_key = str(detected.get("coinmarketcap_api_key") or "")
        vps.firewall = bool(detected.get("firewall"))
        vps.firewall_ssh_port = _safe_int(detected.get("firewall_ssh_port"), 22)
        vps.firewall_ssh_ips = str(detected.get("firewall_ssh_ips") or "")
        vps.init_methode = "password"
        vps.init_status = "successful"
        vps.setup_status = "successful"
        vps.command = "import-existing-vps"
        vps.command_text = "Imported existing VPS"
        vps.user_pw = None
        vps.save()

        self.vpsmanager.vpss.append(vps)
        self.vpsmanager.vpss.sort(key=lambda item: item.hostname or "")

        monitor_enabled = bool(detected.get("key_auth_ok"))
        if monitor_enabled:
            self._set_vps_monitor_enabled(hostname, enabled=True)
            self._refresh_vps_monitor_connection(hostname)
        return {
            "hostname": hostname,
            "config": self._build_vps_config(token, vps),
            "probe": probe,
            "monitor_enabled": monitor_enabled,
            "message": "Imported VPS saved." if monitor_enabled else "Imported VPS saved. Live monitoring needs SSH key authentication.",
        }

    def save_vps_config(self, token: str, hostname: str, form: dict[str, Any]) -> dict[str, Any]:
        vps = self._require_vps(hostname)
        previous_optional = {
            "coinmarketcap_api_key": str(getattr(vps, "coinmarketcap_api_key", "") or "").strip(),
        }
        previous_firewall = {
            "firewall": bool(getattr(vps, "firewall", False)),
            "firewall_ssh_port": _safe_int(getattr(vps, "firewall_ssh_port", 22), 22),
            "firewall_ssh_ips": str(getattr(vps, "firewall_ssh_ips", "") or "").strip(),
        }
        previous_swap = str(getattr(vps, "swap", "0") or "0")
        self._apply_vps_setup_form(token, vps, form)
        vps.save()
        current_optional = {
            "coinmarketcap_api_key": str(getattr(vps, "coinmarketcap_api_key", "") or "").strip(),
        }
        current_firewall = {
            "firewall": bool(getattr(vps, "firewall", False)),
            "firewall_ssh_port": _safe_int(getattr(vps, "firewall_ssh_port", 22), 22),
            "firewall_ssh_ips": str(getattr(vps, "firewall_ssh_ips", "") or "").strip(),
        }
        current_swap = str(getattr(vps, "swap", "0") or "0")
        optional_changed = current_optional != previous_optional
        firewall_changed = current_firewall != previous_firewall
        swap_changed = current_swap != previous_swap
        remote_apply = {"started": False, "command": COMMAND_VPS_APPLY_CONFIG, "command_text": "Apply VPS Config"}
        if optional_changed or firewall_changed or swap_changed:
            if optional_changed:
                self._write_vps_optional_config_pending(vps, current_optional)
            remote_apply = self._start_vps_optional_config_apply(
                token,
                vps,
                apply_optional_config=optional_changed,
                apply_firewall=firewall_changed,
                apply_swap=swap_changed,
            )
        return {
            "config": self._build_vps_config(token, vps),
            "remote_apply": remote_apply,
            "optional_changed": optional_changed,
            "firewall_changed": firewall_changed,
            "swap_changed": swap_changed,
        }

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
        return self._build_vps_progress(vps, include_logs=True)

    def add_vps_to_cluster(self, hostname: str) -> dict[str, Any]:
        """Register one successfully set up VPS as a local Cluster node candidate."""

        vps = self._require_vps(hostname)
        if str(getattr(vps, "setup_status", "") or "") != "successful":
            raise ValueError("Run VPS setup successfully before adding this host to Cluster.")
        result = _register_vps_cluster_node(str(vps.hostname or ""))
        if result.get("ok") is False:
            raise ValueError(str(result.get("error") or "Failed to add VPS to Cluster."))
        return {
            "hostname": str(vps.hostname or ""),
            "cluster": result,
            "cluster_node": self._cluster_node_status(str(vps.hostname or "")),
        }

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
        master_name = _local_master_name()
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
        coinmarketcap_api_key = form.get("coinmarketcap_api_key") if "coinmarketcap_api_key" in form else vps.coinmarketcap_api_key
        self._ensure_coinmarketcap_key_clear_allowed(vps, str(coinmarketcap_api_key or "").strip())
        vps.coinmarketcap_api_key = str(coinmarketcap_api_key or "").strip()
        install_dir = _normalize_vps_install_dir(form.get("install_dir"), vps.user)
        if install_dir:
            vps.remote_pbgui_dir = f"{install_dir}/pbgui"
        vps.firewall = _truthy(form.get("firewall"))
        vps.firewall_ssh_port = _safe_int(form.get("firewall_ssh_port"), 22)
        vps.firewall_ssh_ips = firewall_ips
        vps.save()

    def _is_local_ufw_target(self, hostname: str | None) -> bool:
        value = str(hostname or "").strip()
        return not value or value in {"master", "local", str(getattr(self.vpsmanager, "hostname", "") or "")}

    def _require_remote_master_ufw_target(self, hostname: str) -> None:
        host = str(hostname or "").strip()
        if not host:
            raise ValueError("Remote master hostname is required.")
        monitor = get_monitor()
        if monitor is None or not getattr(monitor, "pool", None):
            raise ValueError("VPS monitor SSH pool is not ready.")
        entry = monitor.pool.get_connection(host)
        if not entry:
            raise ValueError(f"Unknown VPS host: {host}")
        monitor_state = self._get_monitor_state()
        host_state = self._get_host_telemetry(monitor_state, host)
        role = str(self._host_meta(host_state).get("role") or "").strip().lower()
        if role != "master":
            raise ValueError(f"Host {host} is not a master.")

    def _run_local_sudo_shell(self, script: str, sudo_pw: str | None, timeout: int = 30) -> tuple[int, str, str]:
        pw = str(sudo_pw or "")
        if pw:
            command = ["sudo", "-S", "-p", "", "bash", "-lc", script]
            stdin = pw + "\n"
        else:
            command = ["sudo", "-n", "bash", "-lc", script]
            stdin = None
        try:
            proc = subprocess.run(command, input=stdin, text=True, capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            return 124, "", "Command timed out"
        return int(proc.returncode), str(proc.stdout or ""), str(proc.stderr or "")

    def _run_remote_sudo_shell(self, hostname: str, script: str, sudo_pw: str | None, timeout: int = 30) -> tuple[int, str, str]:
        host = str(hostname or "").strip()
        monitor = get_monitor()
        if monitor is None or not getattr(monitor, "pool", None):
            raise ValueError("VPS monitor SSH pool is not ready.")
        loop = getattr(monitor, "loop", None)
        if loop is None or loop.is_closed():
            raise ValueError("VPS monitor loop is not ready.")
        pool = monitor.pool
        pw = str(sudo_pw or "")
        sudo_cmd = f"sudo -S -p '' bash -lc {shlex.quote(script)}" if pw else f"sudo -n bash -lc {shlex.quote(script)}"
        command = "printf '__PBGUISSH_CONNECTION__=%s\\n' \"$SSH_CONNECTION\"; " + sudo_cmd

        async def run() -> tuple[int, str, str]:
            entry = await pool._ensure_live_connection(host)
            if not entry or not getattr(entry, "conn", None):
                return 255, "", "SSH connection is not available"
            result = await entry.conn.run(command, input=(pw + "\n") if pw else None, check=False)
            return int(result.returncode), str(result.stdout or ""), str(result.stderr or "")

        future = asyncio.run_coroutine_threadsafe(run(), loop)
        try:
            return future.result(timeout=timeout + 5)
        except Exception as exc:
            return 124, "", str(exc)

    def _run_ufw_shell(self, hostname: str | None, script: str, sudo_pw: str | None, timeout: int = 30) -> tuple[int, str, str]:
        if self._is_local_ufw_target(hostname):
            return self._run_local_sudo_shell(script, sudo_pw, timeout=timeout)
        host = str(hostname or "").strip()
        self._require_remote_master_ufw_target(host)
        return self._run_remote_sudo_shell(host, script, sudo_pw, timeout=timeout)

    def _extract_ufw_ssh_connection(self, stdout: str) -> tuple[str, str]:
        ssh_connection = ""
        kept: list[str] = []
        for line in str(stdout or "").splitlines():
            if line.startswith("__PBGUISSH_CONNECTION__="):
                ssh_connection = line.split("=", 1)[1].strip()
                continue
            kept.append(line)
        return "\n".join(kept), ssh_connection

    def _format_ufw_command_error(self, stderr: str, stdout: str) -> str:
        combined = "\n".join(part for part in (str(stderr or "").strip(), str(stdout or "").strip()) if part).strip()
        if not combined:
            return "UFW command failed."
        lower = combined.lower()
        if "a password is required" in lower or "password" in lower or "sudo" in lower:
            return "Sudo password required or invalid."
        return combined.splitlines()[0].strip()

    def read_ufw_rules(self, hostname: str | None = None, sudo_pw: str | None = None) -> dict[str, Any]:
        host = str(hostname or "").strip()
        target_kind = "local" if self._is_local_ufw_target(host) else "remote"
        rc, out, err = self._run_ufw_shell(host, "ufw status numbered", sudo_pw, timeout=30)
        out, ssh_connection = self._extract_ufw_ssh_connection(out)
        if rc != 0:
            raise ValueError(self._format_ufw_command_error(err, out))
        parsed = _parse_ufw_numbered_status(out)
        client_ip = _ssh_client_ip_from_connection(ssh_connection)
        return {
            "target": host or "local",
            "target_kind": target_kind,
            "enabled": bool(parsed.get("enabled")),
            "rules": parsed.get("rules") or [],
            "fingerprint": str(parsed.get("fingerprint") or ""),
            "ssh_connection": ssh_connection,
            "ssh_client_ip": client_ip,
            "vpn_management": _is_vpn_management_ip(client_ip),
            "raw": out,
        }

    def _build_ufw_apply_script(self, enabled: bool, delete_numbers: list[int], add_rules: list[dict[str, str]]) -> str:
        commands = ["set -e"]
        for number in sorted({int(item) for item in delete_numbers}, reverse=True):
            commands.append(f"printf 'y\\n' | ufw delete {number}")
        for rule in add_rules:
            port = shlex.quote(str(rule["port"]))
            proto = shlex.quote(str(rule["proto"]))
            from_value = str(rule.get("from") or "Anywhere").strip()
            if from_value.lower() in {"anywhere", "anywhere (v4)", "0.0.0.0/0"}:
                command = f"ufw allow {port}/{proto}"
            else:
                command = f"ufw allow from {shlex.quote(from_value)} to any port {port} proto {proto}"
            comment = str(rule.get("comment") or "").strip()
            if comment:
                command += f" comment {shlex.quote(comment)}"
            commands.append(command)
        commands.append("ufw --force enable" if enabled else "ufw disable")
        commands.append("ufw status numbered")
        return "\n".join(commands)

    def preview_ufw_changes(self, current: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        add_rules = [_normalize_ufw_rule(item) for item in (payload.get("add_rules") or []) if isinstance(item, dict)]
        delete_numbers = sorted({int(item) for item in (payload.get("delete_numbers") or [])})
        enabled = bool(payload.get("enabled"))
        safety = _simulate_ufw_changes(
            list(current.get("rules") or []),
            delete_numbers,
            add_rules,
            enabled,
            str(current.get("ssh_client_ip") or ""),
        )
        return {"enabled": enabled, "delete_numbers": delete_numbers, "add_rules": add_rules, "safety": safety}

    def preview_ufw_rules(self, hostname: str | None, payload: dict[str, Any], sudo_pw: str | None = None) -> dict[str, Any]:
        current = self.read_ufw_rules(hostname, sudo_pw)
        expected_fingerprint = str(payload.get("fingerprint") or "").strip()
        stale = bool(expected_fingerprint and expected_fingerprint != str(current.get("fingerprint") or ""))
        preview = self.preview_ufw_changes(current, payload)
        return {"current": current, "preview": preview, "stale": stale}

    def apply_ufw_rules(self, hostname: str | None, payload: dict[str, Any], sudo_pw: str | None = None) -> dict[str, Any]:
        current = self.read_ufw_rules(hostname, sudo_pw)
        expected_fingerprint = str(payload.get("fingerprint") or "").strip()
        if expected_fingerprint and expected_fingerprint != str(current.get("fingerprint") or ""):
            raise ValueError("UFW rules changed since they were loaded. Refresh UFW before applying changes.")
        preview = self.preview_ufw_changes(current, payload)
        safety = preview["safety"]
        if not safety.get("ok"):
            blocking = safety.get("blocking") or ["UFW safety check failed."]
            raise ValueError(" ".join(str(item) for item in blocking))
        script = self._build_ufw_apply_script(bool(preview["enabled"]), preview["delete_numbers"], preview["add_rules"])
        rc, out, err = self._run_ufw_shell(hostname, script, sudo_pw, timeout=45)
        if rc != 0:
            raise ValueError(self._format_ufw_command_error(err, out))
        refreshed = self.read_ufw_rules(hostname, sudo_pw)
        refreshed["applied"] = {
            "deleted": preview["delete_numbers"],
            "added": preview["add_rules"],
            "enabled": bool(preview["enabled"]),
        }
        refreshed["safety"] = safety
        return refreshed

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

        result = {"hostname": hostname, "hosts_ok": False, "hosts_has_hostname": False, "hosts_current_ip": "", "ssh_ok": False, "ssh_error": ""}

        if not hostname:
            result["ssh_error"] = "Hostname required"
            return result

        lookup = _hosts_entry_lookup(hostname)
        hosts_ip = str(lookup.get("ip") or "").strip()
        if lookup.get("found"):
            result["hosts_has_hostname"] = True
            result["hosts_current_ip"] = hosts_ip
            if not ip and _valid_ipv4(hosts_ip):
                ip = hosts_ip
            if hosts_ip == ip:
                result["hosts_ok"] = True

        if not result["hosts_ok"]:
            if result["hosts_has_hostname"]:
                if ip:
                    result["ssh_error"] = f"Hostname found with IP {result['hosts_current_ip']} instead of {ip}"
                else:
                    result["ssh_error"] = f"Hostname found with IP {result['hosts_current_ip']}, but it is not a valid IPv4 address"
            elif not ip:
                result["ssh_error"] = "IP/hostname not found in local /etc/hosts"
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
        sudo_check = self.validate_local_sudo_password(pw)
        if not sudo_check.get("ok"):
            return {"ok": False, "error": str(sudo_check.get("error") or "Sudo validation failed")}
        try:
            proc = subprocess.Popen(
                ["sudo", "-n", "tee", "/etc/hosts"],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            _, err = proc.communicate(input=content, timeout=15)
            if proc.returncode != 0:
                return {"ok": False, "error": err.strip() or "sudo tee failed"}
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": "sudo tee timed out"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        return {"ok": True, "ip": ip, "hostname": hostname}

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

    def detect_public_ip(self) -> dict[str, Any]:
        from urllib.request import urlopen

        try:
            with urlopen("https://api.ipify.org", timeout=5) as response:  # noqa: S310 - fixed URL
                ip = response.read().decode("utf-8", errors="replace").strip()
        except Exception as exc:
            _log(SERVICE, f"Failed to detect public IP: {exc}", level="WARNING")
            return {"ok": False, "ip": "", "error": "Failed to detect public IP."}
        if not _valid_ipv4(ip):
            return {"ok": False, "ip": "", "error": "Detected public IP is not a valid IPv4 address."}
        return {"ok": True, "ip": ip, "error": ""}
