"""Shared installer orchestration for browser and CLI frontends."""

from __future__ import annotations

from dataclasses import dataclass, field
import configparser
import getpass
import ipaddress
import json
import os
import platform
import re
from pathlib import Path, PurePosixPath
import secrets
import shlex
import shutil
import subprocess
import time
from typing import Callable
from urllib.request import urlopen

from .ssh import SSHConnection

LogCallback = Callable[[str], None]
TOTP_QR_BEGIN = "__PBGUI_TOTP_QR_BEGIN__"
TOTP_QR_END = "__PBGUI_TOTP_QR_END__"
SAFE_INSTALL_PATH_RE = re.compile(r"^[A-Za-z0-9._~/-]+$")
SAFE_GIT_BRANCH_RE = re.compile(r"^[A-Za-z0-9._/-]+$")
LOCAL_APT_PACKAGES = [
    "software-properties-common",
    "ca-certificates",
    "curl",
    "git",
    "python3",
    "python3-pip",
    "python3.12-venv",
    "gcc",
    "build-essential",
    "pkg-config",
]
LOCAL_PREREQUISITE_COMMANDS = {
    "git": "git",
    "curl": "curl",
    "gcc": "gcc/build-essential",
    "pkg-config": "pkg-config",
    "systemctl": "systemd user services",
}
LOCAL_SYSTEMD_UNITS = (
    "pbgui-api.service",
    "pbgui-pbrun.service",
    "pbgui-pbdata.service",
    "pbgui-pbcoindata.service",
    "pbgui-pbremote.service",
)
PBGUI_SERVICE_SCRIPTS = {
    "PBApiServer.py",
    "PBRun.py",
    "PBData.py",
    "PBCoinData.py",
    "PBRemote.py",
}


class RemoteInstallError(RuntimeError):
    """Remote install failure with optional partial result artifacts."""

    def __init__(self, message: str, *, result: dict | None = None) -> None:
        super().__init__(message)
        self.result = result or {}


def default_target_user() -> str:
    """Return the local logged-in user to use as default remote PBGui user."""
    return (getpass.getuser() or os.environ.get("USER") or "pbgui").strip() or "pbgui"


def default_remote_install_dir(target_user: str) -> str:
    """Return the default remote install parent directory for a target user."""
    user = (target_user or default_target_user()).strip() or "pbgui"
    return f"/home/{user}/software"


def default_local_install_dir() -> str:
    """Return the default local install parent shown to users."""
    return _detected_local_install_dir() or "~/software"


def default_local_master_name() -> str:
    """Return the default local PBGui master name."""
    return (platform.node() or "pbgui-local").strip() or "pbgui-local"


def _current_installer_branch() -> str:
    """Return the source checkout branch when the installer is branch-based."""
    env_branch = str(os.environ.get("PBGUI_INSTALLER_BRANCH") or "").strip()
    if env_branch:
        return normalize_installer_branch(env_branch)
    root = _installer_root()
    if not (root / ".git").exists():
        return ""
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--abbrev-ref", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return ""
    branch = str(proc.stdout or "").strip()
    if proc.returncode != 0 or not branch or branch == "HEAD":
        return ""
    try:
        return normalize_installer_branch(branch)
    except ValueError:
        return ""


def normalize_installer_branch(value: str | None) -> str:
    """Validate an optional git branch name for remote shell use."""
    branch = str(value or "").strip()
    if not branch:
        return ""
    if branch.startswith("-") or ".." in branch or not SAFE_GIT_BRANCH_RE.fullmatch(branch):
        raise ValueError("Installer branch contains invalid characters.")
    if any(part in {"", ".", ".."} for part in branch.split("/")):
        raise ValueError("Installer branch contains invalid path segments.")
    return branch


def _validate_safe_install_path_text(raw: str, label: str) -> None:
    """Reject install paths that would break generated shell/systemd files."""
    if any(ch in raw for ch in ("\x00", "\n", "\r")) or "{{" in raw or "}}" in raw:
        raise ValueError(f"{label} contains invalid characters.")
    if not SAFE_INSTALL_PATH_RE.fullmatch(raw):
        raise ValueError(f"{label} may only contain letters, numbers, '/', '.', '_', '-' and '~'.")
    if "." in raw.split("/") or ".." in raw.split("/"):
        raise ValueError(f"{label} cannot contain '.' or '..' path segments.")


def normalize_remote_install_dir(value: str, target_user: str) -> str:
    """Validate and normalize a remote POSIX install parent directory."""
    raw = str(value or "").strip() or default_remote_install_dir(target_user)
    _validate_safe_install_path_text(raw, "Install parent directory")
    path = PurePosixPath(raw)
    if not path.is_absolute():
        raise ValueError("Install parent directory must be an absolute path.")
    if ".." in path.parts:
        raise ValueError("Install parent directory cannot contain '..' path segments.")
    normalized = str(path)
    if normalized == "/":
        raise ValueError("Install parent directory cannot be '/'.")
    _validate_safe_install_path_text(normalized, "Install parent directory")
    return normalized


def normalize_local_install_dir(value: str) -> str:
    """Validate and normalize a local install parent directory."""
    raw = str(value or "").strip() or default_local_install_dir()
    _validate_safe_install_path_text(raw, "Install parent directory")
    raw_path = Path(raw)
    path = raw_path.expanduser()
    if not path.is_absolute():
        raise ValueError("Install parent directory must be an absolute path or start with '~'.")
    normalized = str(path)
    if normalized == "/":
        raise ValueError("Install parent directory cannot be '/'.")
    _validate_safe_install_path_text(normalized, "Install parent directory")
    return normalized


@dataclass
class LocalMasterConfig:
    """Local master installation settings."""

    install_dir: str = field(default_factory=default_local_install_dir)
    local_sudo_password: str = ""
    master_name: str = field(default_factory=default_local_master_name)
    pbgui_password: str = "PBGui$Bot!"
    pbgui_bind_host: str = "127.0.0.1"
    pbgui_port: int = 8000
    start_services: bool = True

    @classmethod
    def from_mapping(cls, data: dict) -> "LocalMasterConfig":
        """Build local config from web/CLI input."""
        return cls(
            install_dir=str(data.get("install_dir") or default_local_install_dir()).strip(),
            local_sudo_password=str(data.get("local_sudo_password") or ""),
            master_name=str(data.get("master_name") or data.get("hostname") or default_local_master_name()).strip(),
            pbgui_password=str(data.get("pbgui_password") or "PBGui$Bot!"),
            pbgui_bind_host=str(data.get("pbgui_bind_host") or "127.0.0.1").strip(),
            pbgui_port=int(data.get("pbgui_port") or 8000),
            start_services=bool(data.get("start_services", True)),
        )

    def validate(self) -> None:
        """Validate local install settings."""
        self.install_dir = normalize_local_install_dir(self.install_dir)
        if not self.master_name:
            raise ValueError("Master name is required.")
        if not (1024 <= int(self.pbgui_port) <= 65535):
            raise ValueError("PBGui port must be between 1024 and 65535.")


@dataclass
class LocalUninstallConfig:
    """Local master uninstall settings."""

    install_dir: str = field(default_factory=default_local_install_dir)
    confirm: bool = False

    @classmethod
    def from_mapping(cls, data: dict) -> "LocalUninstallConfig":
        """Build local uninstall config from web/CLI input."""
        confirm_value = data.get("uninstall_confirm")
        return cls(
            install_dir=str(data.get("install_dir") or default_local_install_dir()).strip(),
            confirm=confirm_value in {True, "true", "1", "yes", "on"},
        )

    def validate(self) -> None:
        """Validate local uninstall settings."""
        self.install_dir = normalize_local_install_dir(self.install_dir)
        if not self.confirm:
            raise ValueError("Local uninstall requires confirmation in the safety dialog.")


@dataclass
class RemoteMasterConfig:
    """Remote master installation settings."""

    remote_host: str
    ssh_port: int = 22
    login_mode: str = "root"
    ssh_username: str = "root"
    ssh_password: str = ""
    root_password: str = ""
    target_user: str = field(default_factory=default_target_user)
    target_password: str = ""
    hostname: str = "pbgui-master"
    swap_size: str = "6G"
    pbgui_password: str = "PBGui$Bot!"
    pbgui_bind_host: str = "0.0.0.0"
    pbgui_port: int = 8000
    openvpn_cidr: str = "10.8.0.0/24"
    ssh_mode: str = "specific_ips_vpn"
    ssh_allowed_ips: list[str] = field(default_factory=list)
    install_dir: str = ""
    coinmarketcap_api_key: str = ""
    enable_pbremote: bool = False
    local_public_key: str = ""
    installer_branch: str = field(default_factory=_current_installer_branch)
    confirm_fresh_host: bool = False
    accept_unknown_host: bool = False
    accepted_host_key_fingerprint: str = ""

    @classmethod
    def from_mapping(cls, data: dict) -> "RemoteMasterConfig":
        """Build config from web/CLI input."""
        allowed_raw = data.get("ssh_allowed_ips") or []
        if isinstance(allowed_raw, str):
            allowed = [item.strip() for item in allowed_raw.replace("\n", ",").split(",") if item.strip()]
        else:
            allowed = [str(item).strip() for item in allowed_raw if str(item).strip()]
        login_mode = str(data.get("login_mode") or "root").strip().lower()
        ssh_username = str(data.get("ssh_username") or ("root" if login_mode == "root" else "")).strip()
        target_user = str(data.get("target_user") or (ssh_username if login_mode == "sudo" else default_target_user())).strip()
        target_password = str(data.get("target_password") or "")
        if login_mode == "sudo" and not target_password:
            target_password = str(data.get("ssh_password") or "")
        install_dir = normalize_remote_install_dir(str(data.get("install_dir") or ""), target_user)
        return cls(
            remote_host=str(data.get("remote_host") or "").strip(),
            ssh_port=int(data.get("ssh_port") or 22),
            login_mode=login_mode,
            ssh_username=ssh_username,
            ssh_password=str(data.get("ssh_password") or ""),
            root_password=str(data.get("root_password") or ""),
            target_user=target_user,
            target_password=target_password,
            hostname=str(data.get("hostname") or "pbgui-master").strip(),
            swap_size=str(data.get("swap_size") or "6G").strip(),
            pbgui_password=str(data.get("pbgui_password") or "PBGui$Bot!"),
            pbgui_bind_host=str(data.get("pbgui_bind_host") or "0.0.0.0").strip(),
            pbgui_port=int(data.get("pbgui_port") or 8000),
            openvpn_cidr=str(data.get("openvpn_cidr") or "10.8.0.0/24").strip(),
            ssh_mode=str(data.get("ssh_mode") or "specific_ips_vpn").strip(),
            ssh_allowed_ips=allowed,
            install_dir=install_dir,
            coinmarketcap_api_key=str(data.get("coinmarketcap_api_key") or "").strip(),
            enable_pbremote=bool(data.get("enable_pbremote")),
            installer_branch=normalize_installer_branch(data.get("installer_branch") or _current_installer_branch()),
            confirm_fresh_host=data.get("confirm_fresh_host") in {True, "true", "1", "yes", "on"},
            accept_unknown_host=data.get("accept_unknown_host") in {True, "true", "1", "yes", "on"},
            accepted_host_key_fingerprint=str(data.get("accepted_host_key_fingerprint") or "").strip(),
        )

    def validate(self) -> None:
        """Validate required settings."""
        if not self.remote_host:
            raise ValueError("Remote host is required.")
        if self.login_mode not in {"root", "sudo"}:
            raise ValueError("Login mode must be root or sudo.")
        if not (1 <= int(self.ssh_port) <= 65535):
            raise ValueError("SSH port must be between 1 and 65535.")
        if not self.ssh_username:
            raise ValueError("SSH username is required.")
        if not self.ssh_password:
            raise ValueError("SSH password is required.")
        if not self.target_user:
            raise ValueError("Target user is required.")
        if not self.confirm_fresh_host:
            raise ValueError("Remote master install requires confirmation that the target is a fresh VPS and host-level services may be changed.")
        if self.login_mode == "root" and not self.target_password:
            raise ValueError("Target user password is required for root installs.")
        self.install_dir = normalize_remote_install_dir(self.install_dir, self.target_user)
        self.installer_branch = normalize_installer_branch(self.installer_branch)
        if self.ssh_mode not in {"specific_ips_vpn", "vpn_only", "anywhere"}:
            raise ValueError("Invalid SSH firewall mode.")
        if self.ssh_mode == "specific_ips_vpn" and not self.ssh_allowed_ips:
            raise ValueError("At least one SSH source IP is required for Specific IPs + VPN mode.")
        if not (1024 <= int(self.pbgui_port) <= 65535):
            raise ValueError("PBGui port must be between 1024 and 65535.")
        network = self.openvpn_network()
        if network.version != 4 or not network.is_private:
            raise ValueError("OpenVPN network must be a private IPv4 CIDR, for example 10.8.0.0/24.")
        if network.prefixlen > 30:
            raise ValueError("OpenVPN network must provide at least two usable addresses.")

    def openvpn_network(self) -> ipaddress.IPv4Network:
        """Return the configured OpenVPN IPv4 network."""
        try:
            network = ipaddress.ip_network(self.openvpn_cidr, strict=True)
        except ValueError as exc:
            raise ValueError("OpenVPN network must be a valid CIDR, for example 10.8.0.0/24.") from exc
        if not isinstance(network, ipaddress.IPv4Network):
            raise ValueError("OpenVPN network must be an IPv4 CIDR.")
        return network

    def openvpn_gateway(self) -> str:
        """Return the OpenVPN server gateway IP."""
        hosts = self.openvpn_network().hosts()
        try:
            return str(next(hosts))
        except StopIteration as exc:
            raise ValueError("OpenVPN network must provide a gateway address.") from exc


def _installer_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _run_command(
    args: list[str | Path],
    log: LogCallback,
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    input_data: str | None = None,
    timeout: int | None = None,
) -> str:
    """Run a local command and stream captured output to the installer log."""
    command = [str(arg) for arg in args]
    log("$ " + shlex.join(command))
    proc = subprocess.run(
        command,
        check=False,
        cwd=str(cwd) if cwd else None,
        env=env,
        input=input_data,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    output = ((proc.stdout or "") + (proc.stderr or "")).strip()
    if output:
        for line in output.splitlines():
            log(line)
    if proc.returncode != 0:
        raise RuntimeError(output or f"Command failed: {shlex.join(command)}")
    return output


def _require_command(name: str, hint: str = "") -> str:
    """Return a command path or raise a clear install error."""
    path = shutil.which(name)
    if not path:
        message = f"Required command not found: {name}"
        if hint:
            message += f". {hint}"
        raise RuntimeError(message)
    return path


def _python312_venv_available() -> bool:
    """Return True when Python 3.12 can create venvs without apt changes."""
    python312 = shutil.which("python3.12")
    if not python312:
        return False
    try:
        proc = subprocess.run(
            [python312, "-c", "import venv, ensurepip"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception:
        return False
    return proc.returncode == 0


def local_prerequisite_status() -> dict[str, object]:
    """Return non-mutating local prerequisite status for the installer UI/CLI."""
    missing: list[str] = []
    for command, label in LOCAL_PREREQUISITE_COMMANDS.items():
        if not shutil.which(command):
            missing.append(label)
    if not _python312_venv_available():
        missing.append("python3.12-venv")

    apt_available = bool(shutil.which("apt-get"))
    sudo_available = bool(shutil.which("sudo"))
    sudo_password_useful = bool(missing and os.getuid() != 0 and apt_available and sudo_available)
    return {
        "ok": not missing,
        "missing": missing,
        "apt_available": apt_available,
        "sudo_available": sudo_available,
        "sudo_password_useful": sudo_password_useful,
    }


def _apt_get_command(sudo_password: str = "") -> tuple[list[str], str | None] | None:
    """Return a non-interactive apt-get command for local prerequisite installs."""
    apt_get = shutil.which("apt-get")
    if not apt_get:
        return None
    if os.getuid() == 0:
        return [apt_get], None
    sudo = shutil.which("sudo")
    if not sudo:
        return None
    password = str(sudo_password or "")
    if password:
        return [sudo, "-S", "-p", "", "env", "DEBIAN_FRONTEND=noninteractive", apt_get], password + "\n"
    return [sudo, "-n", "env", "DEBIAN_FRONTEND=noninteractive", apt_get], None


def _install_local_prerequisites(log: LogCallback, sudo_password: str = "") -> None:
    """Install local master prerequisites on apt-based systems when possible."""
    status = local_prerequisite_status()
    missing = [str(item) for item in status.get("missing") or []]
    if not missing:
        log("Local prerequisites already available; skipping apt install.")
        return
    apt = _apt_get_command(sudo_password)
    if not apt:
        return
    apt_cmd, input_data = apt
    log("Installing missing local prerequisites: " + ", ".join(missing))
    try:
        _run_command([*apt_cmd, "update"], log, input_data=input_data, timeout=300)
        _run_command([*apt_cmd, "install", "-y", *LOCAL_APT_PACKAGES], log, input_data=input_data, timeout=900)
    except RuntimeError as exc:
        raise RuntimeError(
            "Could not install local prerequisites automatically. Run "
            "sudo apt-get update && sudo apt-get install -y git curl build-essential pkg-config python3.12-venv "
            "and retry, or enter your local sudo password in the installer."
        ) from exc


def _ensure_git_checkout(
    repo_url: str,
    target: Path,
    log: LogCallback,
    *,
    current_source: Path | None = None,
    branch: str = "",
) -> None:
    """Clone or fast-forward an existing git checkout."""
    branch = normalize_installer_branch(branch)
    if current_source and target.exists():
        try:
            if target.resolve() == current_source.resolve():
                log(f"Using current PBGui checkout: {target}")
                return
        except OSError:
            pass
    if (target / ".git").exists():
        log(f"Updating existing checkout: {target}")
        if branch:
            _run_command(["git", "fetch", "origin", f"{branch}:refs/remotes/origin/{branch}"], log, cwd=target)
            _run_command(["git", "checkout", "-B", branch, f"refs/remotes/origin/{branch}"], log, cwd=target)
            _run_command(["git", "pull", "--ff-only", "origin", branch], log, cwd=target)
            return
        _run_command(["git", "pull", "--ff-only"], log, cwd=target)
        return
    if target.exists() and not target.is_dir():
        raise RuntimeError(f"Target path exists and is not a directory: {target}")
    if target.exists() and any(target.iterdir()):
        raise RuntimeError(f"Target directory exists and is not an empty git checkout: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    clone_cmd: list[str | Path] = ["git", "clone"]
    if branch:
        clone_cmd.extend(["--branch", branch, "--single-branch"])
    clone_cmd.extend([repo_url, target])
    _run_command(clone_cmd, log)


def _write_pbgui_config(config: LocalMasterConfig, install_dir: Path, pbgui_dir: Path) -> None:
    """Write local PBGui pbgui.ini atomically."""
    cfg = configparser.ConfigParser()
    cfg["main"] = {
        "pbname": config.master_name,
        "pb7dir": str(install_dir / "pb7"),
        "pb7venv": str(install_dir / "venv_pb7" / "bin" / "python"),
        "role": "master",
    }
    cfg["api_server"] = {"host": config.pbgui_bind_host, "port": str(config.pbgui_port)}
    cfg["coinmarketcap"] = {"api_key": "", "fetch_limit": "1000", "fetch_interval": "4"}
    path = pbgui_dir / "pbgui.ini"
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        cfg.write(handle)
    os.replace(tmp, path)


def _write_auth_secret(config: LocalMasterConfig, pbgui_dir: Path) -> None:
    """Write the local PBGui auth secret atomically."""
    path = pbgui_dir / "data" / "auth" / "secrets.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    password = config.pbgui_password.replace("\\", "\\\\").replace('"', '\\"')
    tmp = path.with_suffix(".tmp")
    tmp.write_text(f'password = "{password}"\n', encoding="utf-8")
    os.replace(tmp, path)


def _local_url(bind_host: str, port: int) -> str:
    """Return a browser-friendly URL for a local PBGui install."""
    host = bind_host if bind_host not in {"0.0.0.0", "::", ""} else "127.0.0.1"
    return f"http://{host}:{port}/"


def _local_install_targets(install_dir: Path) -> dict[str, Path]:
    """Return removable local install targets under an install parent."""
    return {
        "PBGui": install_dir / "pbgui",
        "PB7": install_dir / "pb7",
        "PBGui venv": install_dir / "venv_pbgui",
        "PB7 venv": install_dir / "venv_pb7",
    }


def _same_local_path(left: Path, right: Path) -> bool:
    """Compare local paths without requiring either path to exist."""
    try:
        return left.expanduser().resolve(strict=False) == right.expanduser().resolve(strict=False)
    except OSError:
        return str(left.expanduser()) == str(right.expanduser())


def _local_systemd_unit_dir() -> Path:
    """Return the current user's systemd unit directory."""
    return Path.home() / ".config" / "systemd" / "user"


def _local_systemd_unit_paths(unit: str) -> tuple[Path, Path]:
    """Return the unit file and default.target symlink paths for a local unit."""
    unit_dir = _local_systemd_unit_dir()
    return unit_dir / unit, unit_dir / "default.target.wants" / unit


def _pbgui_dir_from_unit_path(value: str) -> Path | None:
    """Return a PBGui checkout directory referenced by a systemd unit value."""
    raw = str(value or "").strip().strip('"')
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        return None
    if path.name == "pbgui":
        return path
    if path.name in PBGUI_SERVICE_SCRIPTS and path.parent.name == "pbgui":
        return path.parent
    return None


def _extract_pbgui_dir_from_unit_text(text: str) -> Path | None:
    """Extract the PBGui checkout path from a generated pbgui systemd unit."""
    for line in str(text or "").splitlines():
        key, sep, value = line.strip().partition("=")
        if not sep:
            continue
        if key == "WorkingDirectory":
            pbgui_dir = _pbgui_dir_from_unit_path(value)
            if pbgui_dir:
                return pbgui_dir
        if key == "ExecStart":
            try:
                tokens = shlex.split(value)
            except ValueError:
                tokens = value.split()
            for token in tokens:
                pbgui_dir = _pbgui_dir_from_unit_path(token)
                if pbgui_dir:
                    return pbgui_dir
    return None


def _local_systemd_unit_pbgui_dir(unit: str) -> Path | None:
    """Return the PBGui checkout path referenced by a local systemd unit."""
    for path in _local_systemd_unit_paths(unit):
        if not path.exists() and not path.is_symlink():
            continue
        try:
            pbgui_dir = _extract_pbgui_dir_from_unit_text(path.read_text(encoding="utf-8"))
        except OSError:
            continue
        if pbgui_dir:
            return pbgui_dir
    return None


def _detected_local_install_dir() -> str:
    """Detect the local install parent from existing PBGui systemd units."""
    for unit in LOCAL_SYSTEMD_UNITS:
        pbgui_dir = _local_systemd_unit_pbgui_dir(unit)
        if pbgui_dir:
            return str(pbgui_dir.parent)
    return ""


def _local_systemd_units_for_install(install_dir: Path, log: LogCallback) -> list[str]:
    """Return only systemd units that are proven to belong to install_dir."""
    selected_pbgui_dir = install_dir / "pbgui"
    matched: list[str] = []
    for unit in LOCAL_SYSTEMD_UNITS:
        paths = _local_systemd_unit_paths(unit)
        if not any(path.exists() or path.is_symlink() for path in paths):
            continue
        unit_pbgui_dir = _local_systemd_unit_pbgui_dir(unit)
        if not unit_pbgui_dir:
            log(f"Skipping systemd unit {unit}: could not verify its PBGui path.")
            continue
        if _same_local_path(unit_pbgui_dir, selected_pbgui_dir):
            matched.append(unit)
            continue
        log(f"Skipping systemd unit {unit}: points to {unit_pbgui_dir}, not selected {selected_pbgui_dir}.")
    return matched


def _run_user_systemctl_best_effort(args: list[str], log: LogCallback) -> None:
    """Run systemctl --user without failing uninstall on missing units/managers."""
    systemctl = shutil.which("systemctl")
    if not systemctl:
        log("systemctl not found; skipping systemd user service cleanup.")
        return
    env = os.environ.copy()
    env.setdefault("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
    command = [systemctl, "--user", *args]
    log("$ " + shlex.join(command))
    try:
        proc = subprocess.run(command, check=False, capture_output=True, text=True, timeout=30, env=env)
    except subprocess.TimeoutExpired:
        log(f"Warning: systemctl --user {' '.join(args)} timed out; continuing cleanup.")
        if args[:1] == ["stop"] and len(args) == 2:
            kill_command = [systemctl, "--user", "kill", "--kill-who=all", "--signal=SIGKILL", args[1]]
            log("$ " + shlex.join(kill_command))
            try:
                kill_proc = subprocess.run(kill_command, check=False, capture_output=True, text=True, timeout=10, env=env)
            except subprocess.TimeoutExpired:
                log(f"Warning: systemctl --user kill {args[1]} timed out.")
                return
            kill_output = ((kill_proc.stdout or "") + (kill_proc.stderr or "")).strip()
            if kill_output:
                for line in kill_output.splitlines():
                    log(line)
            if kill_proc.returncode != 0:
                log(f"Warning: systemctl --user kill {args[1]} exited with {kill_proc.returncode}.")
        return
    output = ((proc.stdout or "") + (proc.stderr or "")).strip()
    if output:
        for line in output.splitlines():
            log(line)
    if proc.returncode != 0:
        log(f"Warning: systemctl --user {' '.join(args)} exited with {proc.returncode}.")


def run_local_master_uninstall(config: LocalUninstallConfig, log: LogCallback, artifact_dir: Path | None = None) -> dict:
    """Uninstall a local PBGui master from this machine."""
    config.validate()
    install_dir = Path(config.install_dir)
    root = _installer_root().resolve()
    targets = _local_install_targets(install_dir)
    for label, target in targets.items():
        try:
            if target.exists() and target.resolve() == root:
                raise RuntimeError(f"Refusing to remove current installer checkout for {label}: {target}")
        except OSError:
            pass

    log(f"Uninstalling local PBGui master under: {install_dir}")
    units = _local_systemd_units_for_install(install_dir, log)
    if not units:
        log("No systemd user units matched the selected install parent; skipping unit cleanup.")
    for unit in units:
        _run_user_systemctl_best_effort(["stop", unit], log)
    for unit in units:
        _run_user_systemctl_best_effort(["disable", unit], log)

    removed_units: list[str] = []
    for unit in units:
        for path in _local_systemd_unit_paths(unit):
            try:
                if path.exists() or path.is_symlink():
                    path.unlink()
                    removed_units.append(str(path))
                    log(f"Removed systemd file: {path}")
            except OSError as exc:
                log(f"Warning: could not remove {path}: {exc}")
    _run_user_systemctl_best_effort(["daemon-reload"], log)
    _run_user_systemctl_best_effort(["reset-failed"], log)

    removed_paths: list[str] = []
    for label, target in targets.items():
        if not target.exists() and not target.is_symlink():
            log(f"Skipping missing {label}: {target}")
            continue
        if target.is_symlink() or target.is_file():
            target.unlink()
        else:
            shutil.rmtree(target)
        removed_paths.append(str(target))
        log(f"Removed {label}: {target}")
    try:
        install_dir.rmdir()
        log(f"Removed empty install parent: {install_dir}")
        removed_paths.append(str(install_dir))
    except OSError:
        log(f"Install parent kept because it is not empty: {install_dir}")

    return {
        "ok": True,
        "mode": "local-uninstall",
        "install_dir": str(install_dir),
        "removed_paths": removed_paths,
        "removed_units": removed_units,
        "completed_at": int(time.time()),
    }


def run_local_master_install(config: LocalMasterConfig, log: LogCallback, artifact_dir: Path | None = None) -> dict:
    """Install a local PBGui master on this machine."""
    config.validate()
    _install_local_prerequisites(log, config.local_sudo_password)
    _require_command("git", "Install git and retry.")
    _require_command("curl", "Install curl and retry.")
    _require_command("gcc", "Install build-essential/gcc and retry.")
    _require_command("systemctl", "systemd user services are required for the local master install.")
    python312 = _require_command("python3.12", "Install Python 3.12 with venv support and retry.")

    install_dir = Path(config.install_dir)
    pbgui_dir = install_dir / "pbgui"
    pb7_dir = install_dir / "pb7"
    pbgui_venv = install_dir / "venv_pbgui"
    pb7_venv = install_dir / "venv_pb7"
    root = _installer_root()

    if artifact_dir:
        artifact_dir.mkdir(parents=True, exist_ok=True)
    log(f"Using local install parent directory: {install_dir}")
    log(f"PBGui: {pbgui_dir}")
    log(f"PB7: {pb7_dir}")
    log(f"Venvs: {pbgui_venv}, {pb7_venv}")

    _ensure_git_checkout(
        "https://github.com/msei99/pbgui.git",
        pbgui_dir,
        log,
        current_source=root,
        branch=_current_installer_branch(),
    )
    _ensure_git_checkout("https://github.com/enarjord/passivbot.git", pb7_dir, log)

    setup_systemd_source = root / "setup" / "setup_systemd.sh"
    setup_systemd_target = pbgui_dir / "setup" / "setup_systemd.sh"
    if not setup_systemd_target.exists() and setup_systemd_source.exists():
        setup_systemd_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(setup_systemd_source, setup_systemd_target)
        setup_systemd_target.chmod(0o755)

    log("Creating Python virtualenvs...")
    _run_command([python312, "-m", "venv", pb7_venv], log)
    _run_command([pb7_venv / "bin" / "python", "-m", "pip", "install", "--upgrade", "pip"], log)
    _run_command([pb7_venv / "bin" / "python", "-m", "pip", "install", "-r", pb7_dir / "requirements.txt"], log)
    _run_command([pb7_venv / "bin" / "python", "-m", "pip", "install", "maturin"], log)

    pbgui_requirements = pbgui_dir / "requirements.txt"
    if not pbgui_requirements.exists():
        pbgui_requirements = pbgui_dir / "requirements_vps.txt"
    _run_command([python312, "-m", "venv", pbgui_venv], log)
    _run_command([pbgui_venv / "bin" / "python", "-m", "pip", "install", "--upgrade", "pip"], log)
    _run_command([pbgui_venv / "bin" / "python", "-m", "pip", "install", "-r", pbgui_requirements], log)

    log("Building passivbot-rust...")
    if not shutil.which("rustup"):
        _run_command(["bash", "-lc", "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal"], log)
    cargo_env = Path.home() / ".cargo" / "env"
    _run_command(["bash", "-lc", f"source {shlex.quote(str(cargo_env))} && rustup toolchain install 1.90.0 && rustup default 1.90.0"], log)
    _run_command(
        [
            "bash",
            "-lc",
            "source "
            + shlex.quote(str(cargo_env))
            + " && source "
            + shlex.quote(str(pb7_venv / "bin" / "activate"))
            + " && cd "
            + shlex.quote(str(pb7_dir / "passivbot-rust"))
            + " && maturin develop --release",
        ],
        log,
    )
    _run_command(
        [
            "bash",
            "-lc",
            "source "
            + shlex.quote(str(pb7_venv / "bin" / "activate"))
            + " && cd "
            + shlex.quote(str(pb7_dir))
            + " && python -c \"import sys; sys.path.insert(0, 'src'); from rust_utils import stamp_compiled_extensions, source_fingerprint; stamp_compiled_extensions(source_fingerprint()); print('Rust source stamp updated.')\"",
        ],
        log,
    )

    log("Writing PBGui configuration...")
    _write_pbgui_config(config, install_dir, pbgui_dir)
    _write_auth_secret(config, pbgui_dir)

    if config.start_services:
        if not setup_systemd_target.exists():
            raise RuntimeError(f"Systemd setup helper not found: {setup_systemd_target}")
        log("Installing PBGui systemd user services...")
        _run_command(
            [
                "bash",
                setup_systemd_target,
                "--user",
                getpass.getuser(),
                "--pbgui-dir",
                pbgui_dir,
                "--python",
                pbgui_venv / "bin" / "python",
                "--enable",
                "api,pbrun,pbdata,pbcoindata",
            ],
            log,
        )

    return {
        "ok": True,
        "mode": "local",
        "install_dir": str(install_dir),
        "pbgui_dir": str(pbgui_dir),
        "pb7_dir": str(pb7_dir),
        "pbgui_python": str(pbgui_venv / "bin" / "python"),
        "pb7_python": str(pb7_venv / "bin" / "python"),
        "local_url": _local_url(config.pbgui_bind_host, config.pbgui_port),
        "completed_at": int(time.time()),
    }


def ensure_local_public_key(log: LogCallback) -> str:
    """Return a local SSH public key, generating one if necessary."""
    ssh_dir = Path.home() / ".ssh"
    ssh_dir.mkdir(mode=0o700, exist_ok=True)
    candidates = [ssh_dir / "id_ed25519.pub", ssh_dir / "id_rsa.pub"]
    for candidate in candidates:
        if candidate.exists():
            return candidate.read_text(encoding="utf-8").strip()
    key_path = ssh_dir / "id_ed25519"
    if not shutil.which("ssh-keygen"):
        raise RuntimeError("ssh-keygen is required to generate a local SSH key.")
    log("No local SSH key found. Generating ~/.ssh/id_ed25519 ...")
    subprocess.run(["ssh-keygen", "-t", "ed25519", "-f", str(key_path), "-N", "", "-C", "pbgui-installer"], check=True)
    return key_path.with_suffix(".pub").read_text(encoding="utf-8").strip()


def detect_public_ip(timeout: float = 5.0) -> str:
    """Return the current public IPv4/IPv6 address if it can be detected."""
    try:
        with urlopen("https://api.ipify.org", timeout=timeout) as response:  # noqa: S310 - fixed URL
            return response.read().decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def run_remote_master_install(config: RemoteMasterConfig, log: LogCallback, artifact_dir: Path) -> dict:
    """Install a remote PBGui master over SSH."""
    config.validate()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    config.local_public_key = ensure_local_public_key(log)

    root = _installer_root()
    script_path = root / "setup" / "installer" / "scripts" / "remote_master_bootstrap.sh"
    systemd_setup_path = root / "setup" / "setup_systemd.sh"
    if not script_path.exists():
        raise FileNotFoundError(f"Remote bootstrap script not found: {script_path}")
    if not systemd_setup_path.exists():
        raise FileNotFoundError(f"Systemd setup helper not found: {systemd_setup_path}")

    token = secrets.token_hex(8)
    remote_script = f"/tmp/pbgui_remote_master_bootstrap_{token}.sh"
    remote_systemd_setup = f"/tmp/pbgui_setup_systemd_{token}.sh"
    remote_config = f"/tmp/pbgui_remote_master_config_{token}.json"
    remote_result = f"/tmp/pbgui_remote_master_result_{token}.json"

    payload = config.__dict__.copy()
    payload["result_path"] = remote_result
    payload["uploaded_setup_systemd_path"] = remote_systemd_setup

    log(f"Connecting to {config.ssh_username}@{config.remote_host}:{config.ssh_port} ...")
    with SSHConnection(
        host=config.remote_host,
        port=config.ssh_port,
        username=config.ssh_username,
        password=config.ssh_password,
        accept_unknown_host=config.accept_unknown_host,
        expected_host_key_fingerprint=config.accepted_host_key_fingerprint,
    ) as conn:
        conn.put_text(remote_config, json.dumps(payload, indent=2), mode=0o600)
        conn.put_file(script_path, remote_script, mode=0o700)
        conn.put_file(systemd_setup_path, remote_systemd_setup, mode=0o700)
        if config.login_mode == "sudo":
            command = f"sudo -S -p '' bash {remote_script} {remote_config}"
            sudo_password = config.ssh_password
        else:
            command = f"bash {remote_script} {remote_config}"
            sudo_password = None
        rc = conn.run_stream(command, log=log, sudo_password=sudo_password, timeout=None)

        result = {}
        try:
            raw_result = conn.read_text(remote_result)
            result = json.loads(raw_result) if raw_result.strip() else {}
        except Exception as exc:
            log(f"Warning: could not read remote result file: {exc}")

        profile_name = "".join(
            ch if ch.isascii() and (ch.isalnum() or ch in "._-") else "_"
            for ch in config.hostname
        ).strip("_") or "pbgui-master"
        ovpn_remote = result.get("ovpn_path") or f"/home/{config.target_user}/{profile_name}_client/{profile_name}.ovpn"
        qr_remote = result.get("totp_qr_path") or f"/home/{config.target_user}/GA-QR.txt"
        ovpn_local = artifact_dir / (Path(ovpn_remote).name or f"{profile_name}.ovpn")
        qr_local = artifact_dir / "GA-QR.txt"
        try:
            conn.get_file(ovpn_remote, ovpn_local)
            result["ovpn_local"] = str(ovpn_local)
            result["openvpn_cidr"] = config.openvpn_cidr
            log(f"Downloaded OpenVPN profile: {ovpn_local}")
        except Exception as exc:
            log(f"Warning: could not download OpenVPN profile: {exc}")
        try:
            conn.get_file(qr_remote, qr_local)
            result["totp_qr_local"] = str(qr_local)
            log("Received TOTP QR text for inline display.")
        except Exception as exc:
            log(f"Warning: could not download TOTP QR text: {exc}")

        conn.run_stream(f"rm -f {remote_script} {remote_systemd_setup} {remote_config} {remote_result}", log=lambda _msg: None, timeout=30)

        if rc != 0:
            raise RemoteInstallError(f"Remote bootstrap failed with exit code {rc}.", result=result)

    result.setdefault("vpn_url", f"http://{config.openvpn_gateway()}:{config.pbgui_port}/")
    result.setdefault("public_url", f"http://{config.remote_host}:{config.pbgui_port}/")
    result.setdefault("openvpn_cidr", config.openvpn_cidr)
    result["completed_at"] = int(time.time())
    return result
