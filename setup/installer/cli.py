"""Terminal frontend for the PBGui master installer."""

from __future__ import annotations

from getpass import getpass
from pathlib import Path
import tempfile

from .core import TOTP_QR_BEGIN, TOTP_QR_END, RemoteMasterConfig, default_target_user, detect_public_ip, run_remote_master_install


def _ask(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{prompt}{suffix}: ").strip()
    return value or default


def _ask_password(prompt: str, default: str = "") -> str:
    value = getpass(f"{prompt}: ")
    return value or default


def run_cli() -> int:
    """Run the CLI installer."""
    print("PBGui Master Installer")
    print("1) Remote Master VPS")
    mode = _ask("Select mode", "1")
    if mode != "1":
        print("Only Remote Master VPS is implemented in this phase.")
        return 2

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
    remote_host = _ask("VPS IP or hostname")
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
            "ssh_port": 22,
            "login_mode": login_mode,
            "ssh_username": ssh_username,
            "ssh_password": ssh_password,
            "root_password": root_password,
            "target_user": target_user,
            "target_password": target_password,
            "hostname": hostname,
            "swap_size": swap_size,
            "pbgui_password": pbgui_password,
            "pbgui_bind_host": pbgui_bind_host,
            "pbgui_port": pbgui_port,
            "openvpn_cidr": openvpn_cidr,
            "ssh_mode": ssh_mode,
            "ssh_allowed_ips": ssh_allowed_ips,
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
