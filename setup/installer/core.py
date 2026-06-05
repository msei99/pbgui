"""Shared installer orchestration for browser and CLI frontends."""

from __future__ import annotations

from dataclasses import dataclass, field
import configparser
import getpass
import ipaddress
import json
import os
import platform
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
    return "~/software"


def default_local_master_name() -> str:
    """Return the default local PBGui master name."""
    return (platform.node() or "pbgui-local").strip() or "pbgui-local"


def normalize_remote_install_dir(value: str, target_user: str) -> str:
    """Validate and normalize a remote POSIX install parent directory."""
    raw = str(value or "").strip() or default_remote_install_dir(target_user)
    if any(ch in raw for ch in ("\x00", "\n", "\r")):
        raise ValueError("Install parent directory contains invalid control characters.")
    if "'" in raw:
        raise ValueError("Install parent directory cannot contain single quotes.")
    path = PurePosixPath(raw)
    if not path.is_absolute():
        raise ValueError("Install parent directory must be an absolute path.")
    if ".." in path.parts:
        raise ValueError("Install parent directory cannot contain '..' path segments.")
    normalized = str(path)
    if normalized == "/":
        raise ValueError("Install parent directory cannot be '/'.")
    return normalized


def normalize_local_install_dir(value: str) -> str:
    """Validate and normalize a local install parent directory."""
    raw = str(value or "").strip() or default_local_install_dir()
    if any(ch in raw for ch in ("\x00", "\n", "\r")):
        raise ValueError("Install parent directory contains invalid control characters.")
    raw_path = Path(raw)
    if ".." in raw_path.parts:
        raise ValueError("Install parent directory cannot contain '..' path segments.")
    path = raw_path.expanduser()
    if not path.is_absolute():
        raise ValueError("Install parent directory must be an absolute path or start with '~'.")
    normalized = str(path)
    if normalized == "/":
        raise ValueError("Install parent directory cannot be '/'.")
    return normalized


@dataclass
class LocalMasterConfig:
    """Local master installation settings."""

    install_dir: str = field(default_factory=default_local_install_dir)
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
    confirm_text: str = ""

    @classmethod
    def from_mapping(cls, data: dict) -> "LocalUninstallConfig":
        """Build local uninstall config from web/CLI input."""
        confirm_value = data.get("uninstall_confirm")
        return cls(
            install_dir=str(data.get("install_dir") or default_local_install_dir()).strip(),
            confirm=confirm_value in {True, "true", "1", "yes", "on"},
            confirm_text=str(data.get("uninstall_confirm_text") or "").strip(),
        )

    def validate(self) -> None:
        """Validate local uninstall settings."""
        self.install_dir = normalize_local_install_dir(self.install_dir)
        if not self.confirm or self.confirm_text != "DELETE":
            raise ValueError("Local uninstall requires checking the confirmation box and typing DELETE.")


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
        )

    def validate(self) -> None:
        """Validate required settings."""
        if not self.remote_host:
            raise ValueError("Remote host is required.")
        if self.login_mode not in {"root", "sudo"}:
            raise ValueError("Login mode must be root or sudo.")
        if not self.ssh_username:
            raise ValueError("SSH username is required.")
        if not self.ssh_password:
            raise ValueError("SSH password is required.")
        if not self.target_user:
            raise ValueError("Target user is required.")
        if self.login_mode == "root" and not self.target_password:
            raise ValueError("Target user password is required for root installs.")
        self.install_dir = normalize_remote_install_dir(self.install_dir, self.target_user)
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


def _apt_get_command() -> list[str] | None:
    """Return a non-interactive apt-get command for local prerequisite installs."""
    apt_get = shutil.which("apt-get")
    if not apt_get:
        return None
    if os.getuid() == 0:
        return [apt_get]
    sudo = shutil.which("sudo")
    if not sudo:
        return None
    return [sudo, "-n", "env", "DEBIAN_FRONTEND=noninteractive", apt_get]


def _install_local_prerequisites(log: LogCallback) -> None:
    """Install local master prerequisites on apt-based systems when possible."""
    apt_cmd = _apt_get_command()
    if not apt_cmd:
        return
    packages = [
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
    log("Ensuring local installer prerequisites are installed...")
    try:
        _run_command([*apt_cmd, "update"], log, timeout=300)
        _run_command([*apt_cmd, "install", "-y", *packages], log, timeout=900)
    except RuntimeError as exc:
        raise RuntimeError(
            "Could not install local prerequisites automatically. Run "
            "sudo apt-get update && sudo apt-get install -y git curl build-essential pkg-config python3.12-venv "
            "and retry. If you just started the browser installer, run sudo -v in the same terminal first."
        ) from exc


def _ensure_git_checkout(repo_url: str, target: Path, log: LogCallback, *, current_source: Path | None = None) -> None:
    """Clone or fast-forward an existing git checkout."""
    if current_source and target.exists():
        try:
            if target.resolve() == current_source.resolve():
                log(f"Using current PBGui checkout: {target}")
                return
        except OSError:
            pass
    if (target / ".git").exists():
        log(f"Updating existing checkout: {target}")
        _run_command(["git", "pull", "--ff-only"], log, cwd=target)
        return
    if target.exists() and not target.is_dir():
        raise RuntimeError(f"Target path exists and is not a directory: {target}")
    if target.exists() and any(target.iterdir()):
        raise RuntimeError(f"Target directory exists and is not an empty git checkout: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    _run_command(["git", "clone", repo_url, target], log)


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
    proc = subprocess.run(command, check=False, capture_output=True, text=True, timeout=30, env=env)
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
    units = [
        "pbgui-api.service",
        "pbgui-pbrun.service",
        "pbgui-pbdata.service",
        "pbgui-pbcoindata.service",
        "pbgui-pbremote.service",
    ]
    for unit in units:
        _run_user_systemctl_best_effort(["stop", unit], log)
    for unit in units:
        _run_user_systemctl_best_effort(["disable", unit], log)

    unit_dir = Path.home() / ".config" / "systemd" / "user"
    wants_dir = unit_dir / "default.target.wants"
    removed_units: list[str] = []
    for unit in units:
        for path in (unit_dir / unit, wants_dir / unit):
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
    _install_local_prerequisites(log)
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

    _ensure_git_checkout("https://github.com/msei99/pbgui.git", pbgui_dir, log, current_source=root)
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
