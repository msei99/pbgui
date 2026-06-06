"""Terminal frontend for the PBGui master installer."""

from __future__ import annotations

from getpass import getpass
import os
from pathlib import Path
import shutil
import tempfile

from .core import (
    LocalMasterConfig,
    TOTP_QR_BEGIN,
    TOTP_QR_END,
    RemoteMasterConfig,
    default_local_install_dir,
    default_local_master_name,
    default_remote_install_dir,
    default_target_user,
    detect_public_ip,
    run_local_master_install,
    run_remote_master_install,
)
from .ssh import probe_ssh_host_key


def _ask(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{prompt}{suffix}: ").strip()
    return value or default


def _ask_password(prompt: str, default: str = "") -> str:
    value = getpass(f"{prompt}: ")
    return value or default


def _print_install_preview(install_dir: str) -> None:
    """Print the install target preview."""
    install_parent = install_dir.rstrip("/")
    print(f"PBGui: {install_parent}/pbgui")
    print(f"PB7: {install_parent}/pb7")
    print(f"Venvs: {install_parent}/venv_pbgui, {install_parent}/venv_pb7")


def _fingerprint_matches(value: str, fingerprint: str) -> bool:
    """Return True when user input matches a SHA256 SSH fingerprint."""
    entered = str(value or "").strip()
    expected = str(fingerprint or "").strip()
    return entered == expected or entered == expected.removeprefix("SHA256:")


def _run_remote_cli() -> int:
    """Run the remote CLI installer flow."""
    login_mode = _ask("Initial login mode (root/sudo)", "root").lower()
    ssh_username = "root" if login_mode == "root" else _ask("Existing sudo user")
    ssh_password = _ask_password("SSH password")
    root_password = ""
    if login_mode == "root":
        root_password = _ask_password("New root password (optional, unchanged if empty)")
    target_user = _ask("Target PBGui user", default_target_user() if login_mode == "root" else ssh_username)
    target_password = ""
    if login_mode == "root":
        target_password = _ask_password(f"Password for new user {target_user}")
    install_dir = _ask("Install parent directory", default_remote_install_dir(target_user))
    _print_install_preview(install_dir)
    remote_host = _ask("VPS IP or hostname")
    try:
        ssh_port = int(_ask("SSH port", "22"))
    except ValueError:
        print("SSH port must be a number.")
        return 2
    if not (1 <= ssh_port <= 65535):
        print("SSH port must be between 1 and 65535.")
        return 2
    print("\nRemote Master install is intended for a fresh VPS and may change hostname, users, swap, firewall, OpenVPN, packages, and systemd services.")
    fresh_confirm = _ask("Type FRESH_VPS to confirm this target is disposable/fresh", "")
    if fresh_confirm != "FRESH_VPS":
        print("Cancelled.")
        return 2
    try:
        host_key = probe_ssh_host_key(remote_host, ssh_port)
    except Exception as exc:
        print(f"Could not read SSH host key: {exc}")
        return 2
    if host_key.get("mismatch"):
        print("SSH host key mismatch. Refusing to connect until ~/.ssh/known_hosts is fixed intentionally.")
        print(f"Presented key: {host_key.get('key_type')} {host_key.get('fingerprint')}")
        return 2
    accept_unknown_host = False
    accepted_host_key_fingerprint = ""
    if not host_key.get("known"):
        fingerprint = str(host_key.get("fingerprint") or "")
        print("\nThis SSH host key is not in your known_hosts file.")
        print(f"Host key: {host_key.get('key_type')} {fingerprint}")
        confirm_key = _ask("Type the fingerprint above to trust this host for this install", "")
        if not _fingerprint_matches(confirm_key, fingerprint):
            print("Cancelled.")
            return 2
        accept_unknown_host = True
        accepted_host_key_fingerprint = fingerprint
    else:
        print("SSH host key is already known.")
    hostname = _ask("Remote hostname / OpenVPN profile name", "pbgui-master")
    swap_size = _ask("Swap size", "6G")
    pbgui_password = _ask_password("PBGui web password", "PBGui$Bot!")
    pbgui_bind_host = _ask("PBGui bind address", "0.0.0.0")
    pbgui_port = int(_ask("PBGui port", "8000"))
    openvpn_cidr = _ask("OpenVPN network CIDR", "10.8.0.0/24")
    print("SSH firewall modes:")
    print("1) Specific IPs + VPN (Recommended)")
    print("2) VPN only (Most secure)")
    print("3) Allow SSH from everywhere (Not secure, not recommended)")
    ssh_choice = _ask("Select SSH mode", "1")
    ssh_mode = {"1": "specific_ips_vpn", "2": "vpn_only", "3": "anywhere"}.get(ssh_choice, "specific_ips_vpn")
    if ssh_mode == "anywhere":
        confirm = _ask("Type I_UNDERSTAND to allow public SSH", "")
        if confirm != "I_UNDERSTAND":
            print("Cancelled.")
            return 2
    ssh_allowed_ips = ""
    if ssh_mode == "specific_ips_vpn":
        detected_ip = detect_public_ip()
        ssh_allowed_ips = _ask("Allowed SSH source IPs (comma-separated)", detected_ip)

    cfg = RemoteMasterConfig.from_mapping(
        {
            "remote_host": remote_host,
            "ssh_port": ssh_port,
            "login_mode": login_mode,
            "ssh_username": ssh_username,
            "ssh_password": ssh_password,
            "root_password": root_password,
            "target_user": target_user,
            "target_password": target_password,
            "install_dir": install_dir,
            "hostname": hostname,
            "swap_size": swap_size,
            "pbgui_password": pbgui_password,
            "pbgui_bind_host": pbgui_bind_host,
            "pbgui_port": pbgui_port,
            "openvpn_cidr": openvpn_cidr,
            "ssh_mode": ssh_mode,
            "ssh_allowed_ips": ssh_allowed_ips,
            "confirm_fresh_host": True,
            "accept_unknown_host": accept_unknown_host,
            "accepted_host_key_fingerprint": accepted_host_key_fingerprint,
        }
    )
    artifact_dir = Path(tempfile.mkdtemp(prefix="pbgui-installer-"))
    qr_capture: list[str] | None = None

    def log(message: str) -> None:
        nonlocal qr_capture
        text = str(message).rstrip("\r")
        if text == TOTP_QR_BEGIN:
            qr_capture = []
            return
        if text == TOTP_QR_END:
            if qr_capture:
                print("\nTOTP QR code:")
                print("\n".join(qr_capture))
            qr_capture = None
            return
        if qr_capture is not None:
            qr_capture.append(text)
            return
        print(text)

    result = run_remote_master_install(cfg, log, artifact_dir)
    print("\nInstallation complete.")
    print(f"VPN URL: {result.get('vpn_url')}")
    if result.get("ovpn_local"):
        print(f"OpenVPN profile: {result['ovpn_local']}")
    return 0


def _run_local_cli() -> int:
    """Run the local CLI installer flow."""
    install_dir = _ask("Install parent directory", default_local_install_dir())
    _print_install_preview(install_dir)
    local_sudo_password = ""
    if os.getuid() != 0 and shutil.which("apt-get") and shutil.which("sudo"):
        local_sudo_password = _ask_password("Local sudo password for apt prerequisites (leave empty to use an existing sudo session)")
    master_name = _ask("Master name", default_local_master_name())
    pbgui_password = _ask_password("PBGui web password", "PBGui$Bot!")
    pbgui_bind_host = _ask("PBGui bind address", "127.0.0.1")
    pbgui_port = int(_ask("PBGui port", "8000"))
    cfg = LocalMasterConfig.from_mapping(
        {
            "install_dir": install_dir,
            "local_sudo_password": local_sudo_password,
            "master_name": master_name,
            "pbgui_password": pbgui_password,
            "pbgui_bind_host": pbgui_bind_host,
            "pbgui_port": pbgui_port,
        }
    )
    artifact_dir = Path(tempfile.mkdtemp(prefix="pbgui-installer-"))
    result = run_local_master_install(cfg, print, artifact_dir)
    print("\nLocal installation complete.")
    print(f"PBGui URL: {result.get('local_url')}")
    print(f"PBGui directory: {result.get('pbgui_dir')}")
    print(f"PB7 directory: {result.get('pb7_dir')}")
    return 0


def run_cli() -> int:
    """Run the CLI installer."""
    print("PBGui Master Installer")
    print("1) Remote Master VPS")
    print("2) Local Master Install")
    mode = _ask("Select mode", "1")
    if mode == "2":
        return _run_local_cli()
    if mode != "1":
        print("Invalid mode.")
        return 2
    return _run_remote_cli()
