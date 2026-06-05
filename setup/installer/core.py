"""Shared installer orchestration for browser and CLI frontends."""

from __future__ import annotations

from dataclasses import dataclass, field
import getpass
import ipaddress
import json
import os
from pathlib import Path
import secrets
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
            install_dir=str(data.get("install_dir") or "").strip(),
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
