import configparser
import glob
import json
import os
import platform
import re
import shlex
import shutil
import socket
import subprocess
import sys
from datetime import datetime
from pathlib import Path, PurePath

import ansible_runner
import getpass
import paramiko

from logging_helpers import human_log as _log
from pbgui_purefunc import load_ini, pb7dir, pb7venv, save_ini

PBGDIR = Path(__file__).resolve().parent
PB7DIR = pb7dir()
PB7VENV = pb7venv()
TASK_LOG_HISTORY_DEFAULT = 10


def _ansible_envvars() -> dict[str, str]:
    envvars = dict(os.environ)
    candidate_bins = [
        Path(sys.executable).parent,
        Path(sys.executable).resolve().parent,
        PBGDIR.parent / "venv_pbgui" / "bin",
        PBGDIR.parent / "venv_pbgui312" / "bin",
    ]
    ansible_bin = None
    for candidate in candidate_bins:
        if (candidate / "ansible-playbook").exists():
            ansible_bin = candidate
            break
    if ansible_bin is None:
        resolved = shutil.which("ansible-playbook")
        if not resolved:
            return envvars
        ansible_bin = Path(resolved).parent
    ansible_bin_str = str(ansible_bin)
    current_path = envvars.get("PATH", "")
    path_parts = current_path.split(os.pathsep) if current_path else []
    if ansible_bin_str not in path_parts:
        envvars["PATH"] = ansible_bin_str + (os.pathsep + current_path if current_path else "")
    return envvars


def strip_ansi(text: str) -> str:
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    return ansi_escape.sub("", text or "")


def _task_log_history_limit() -> int:
    raw_value = str(load_ini("vps_manager", "task_log_history") or "").strip()
    if not raw_value:
        return TASK_LOG_HISTORY_DEFAULT
    try:
        return max(int(raw_value), 0)
    except ValueError:
        return TASK_LOG_HISTORY_DEFAULT


def _task_log_stem(task_name: str | None, fallback: str) -> str:
    raw_value = str(task_name or fallback or "").strip().lower()
    normalized = re.sub(r"[^a-z0-9_-]+", "-", raw_value).strip("-")
    return normalized or fallback


def _set_vps_monitor_enabled(hostname: str, *, enabled: bool) -> None:
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


def _coerce_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _install_dir_from_remote_pbgui_dir(remote_pbgui_dir: str | None, vps_user: str | None = None) -> str:
    raw = str(remote_pbgui_dir or "").strip().rstrip("/")
    home = f"/home/{vps_user}" if vps_user else str(Path.home())
    if not raw:
        return f"{home}/software"
    if raw.startswith("~/"):
        raw = raw[2:]
    if raw.startswith("/"):
        return str(PurePath(raw).parent)
    if "/" in raw:
        parent = raw.rsplit("/", 1)[0]
        return f"{home}/{parent}" if parent else home
    return home


def _task_log_path(base_dir: Path, task_name: str | None, fallback: str) -> Path:
    return base_dir / f"{_task_log_stem(task_name, fallback)}.log"


def _task_run_log_path(base_dir: Path, task_name: str | None, run_id: str | None, fallback: str) -> Path:
    stem = _task_log_stem(task_name, fallback)
    clean_run_id = re.sub(r"[^a-z0-9_-]+", "-", str(run_id or "").strip().lower()).strip("-")
    if not clean_run_id:
        return _task_log_path(base_dir, task_name, fallback)
    return base_dir / f"{stem}--{clean_run_id}.log"


def _task_log_header(*, task_name: str | None, fallback: str, target: str) -> str:
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    command = _task_log_stem(task_name, fallback)
    return (
        f"=== PLAYBOOK RUN START {started_at} ===\n"
        f"Target: {target}\n"
        f"Task: {command}\n"
        "========================================\n"
    )


def _rotated_task_log_path(path: Path, index: int) -> Path:
    return path.with_name(f"{path.name}.{index}")


def _rotate_task_log(path: Path, history_count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if history_count <= 0:
        path.unlink(missing_ok=True)
        return
    _rotated_task_log_path(path, history_count).unlink(missing_ok=True)
    for index in range(history_count - 1, 0, -1):
        src = _rotated_task_log_path(path, index)
        dst = _rotated_task_log_path(path, index + 1)
        if src.exists():
            src.replace(dst)
    if path.exists():
        path.replace(_rotated_task_log_path(path, 1))


def _prepare_runner_private_data_dir(base_dir: Path) -> Path:
    private_dir = base_dir / "tmp"
    shutil.rmtree(private_dir, ignore_errors=True)
    private_dir.mkdir(parents=True, exist_ok=True)
    return private_dir


class VPS:
    def __init__(self):
        self._hostname = None
        self.path = None
        self.privat_data_dir = None
        self.ip = None
        self.root_pw = None
        self.initial_root_pw = None
        self.user = getpass.getuser()
        self.user_pw = None
        self.private_key_user = None
        self.private_key_file = None
        self.user_sudo = None
        self.user_sudo_pw = None
        self.init_methode = "root"
        self.remove_user = False
        self.swap = "2G"
        self.last_init = None
        self.last_setup = None
        self.last_update = None
        self.init_status = None
        self.setup_status = None
        self.update_status = None
        self.command = "unknown"
        self.command_text = "unknown"
        self.command_run_id = None
        self.reboot = False
        self.init_log = ""
        self.setup_log = ""
        self.update_log = ""
        self.bucket = None
        self.coinmarketcap_api_key = None
        self.firewall = True
        self.firewall_ssh_port = 22
        self.firewall_ssh_ips = ""
        self.logfilename = None
        self.logfile = None
        self.logsize = 50
        self.remote_pbgui_dir = None

    @property
    def hostname(self):
        return self._hostname

    @hostname.setter
    def hostname(self, new_hostname):
        self._hostname = new_hostname
        self.path = Path(f"{PBGDIR}/data/vpsmanager/hosts/{self.hostname}")

    def _task_log_path(self, task_name: str | None = None, fallback: str = "vps-update") -> Path:
        base_dir = self.path or Path(f"{PBGDIR}/data/vpsmanager/hosts/{self.hostname}")
        return _task_run_log_path(base_dir, task_name or self.command, self.command_run_id, fallback)

    def _task_log_alias_path(self, task_name: str | None = None, fallback: str = "vps-update") -> Path:
        base_dir = self.path or Path(f"{PBGDIR}/data/vpsmanager/hosts/{self.hostname}")
        return _task_log_path(base_dir, task_name or self.command, fallback)

    def _rotate_task_log(self, task_name: str | None = None, fallback: str = "vps-update") -> None:
        _rotate_task_log(self._task_log_alias_path(task_name, fallback), _task_log_history_limit())

    def _append_task_log(self, dump: str, *, task_name: str | None = None, fallback: str, buffer_attr: str) -> None:
        log = self._task_log_path(task_name, fallback)
        alias_log = self._task_log_alias_path(task_name, fallback)
        with open(log, "a", encoding="utf-8") as logfile:
            logfile.write(dump)
        try:
            shutil.copyfile(log, alias_log)
        except Exception:
            pass
        setattr(self, buffer_attr, getattr(self, buffer_attr) + dump)

    def _start_task_log(self, task_name: str | None = None, fallback: str = "vps-update") -> None:
        log = self._task_log_path(task_name, fallback)
        alias_log = self._task_log_alias_path(task_name, fallback)
        log.parent.mkdir(parents=True, exist_ok=True)
        header = _task_log_header(task_name=task_name or self.command, fallback=fallback, target=str(self.hostname or "unknown"))
        with open(log, "w", encoding="utf-8") as logfile:
            logfile.write(header)
        try:
            shutil.copyfile(log, alias_log)
        except Exception:
            pass

    def load(self, file_path):
        with open(file_path, "r", encoding="utf-8") as handle:
            config = json.load(handle)
        if "_hostname" in config:
            self._hostname = config["_hostname"]
            self.path = Path(f"{PBGDIR}/data/vpsmanager/hosts/{self.hostname}")
        if "ip" in config:
            self.ip = config["ip"]
        if "user" in config:
            self.user = config["user"]
        if "swap" in config:
            self.swap = config["swap"]
        if "last_setup" in config:
            self.last_setup = config["last_setup"]
        if "last_init" in config:
            self.last_init = config["last_init"]
        if "last_update" in config:
            self.last_update = config["last_update"]
        if "setup_status" in config:
            self.setup_status = config["setup_status"]
        if "init_status" in config:
            self.init_status = config["init_status"]
        if "update_status" in config:
            self.update_status = config["update_status"]
        if "bucket" in config:
            self.bucket = config["bucket"]
        if "coinmarketcap_api_key" in config:
            self.coinmarketcap_api_key = config["coinmarketcap_api_key"]
        if "firewall" in config:
            self.firewall = config["firewall"]
        if "firewall_ssh_port" in config:
            self.firewall_ssh_port = config["firewall_ssh_port"]
        if "firewall_ssh_ips" in config:
            self.firewall_ssh_ips = config["firewall_ssh_ips"]
        if "command" in config:
            self.command = config["command"]
        if "command_text" in config:
            self.command_text = config["command_text"]
        if "command_run_id" in config:
            self.command_run_id = config["command_run_id"]
        if "init_methode" in config:
            self.init_methode = config["init_methode"]
        if "remove_user" in config:
            self.remove_user = config["remove_user"]
        if "private_key_user" in config:
            self.private_key_user = config["private_key_user"]
        if "private_key_file" in config:
            self.private_key_file = config["private_key_file"]
        if "remote_pbgui_dir" in config:
            self.remote_pbgui_dir = config["remote_pbgui_dir"]

    def is_vps_in_hosts(self):
        hosts = Path("/etc/hosts")
        if hosts.exists():
            with open(hosts, "r", encoding="utf-8") as handle:
                for line in handle:
                    found = re.search(f"^{self.ip}[ \t]+{self.hostname}$", line)
                    if found:
                        return True
        return False

    def fetch_vps_ip_from_hosts(self):
        hosts = Path("/etc/hosts")
        if hosts.exists():
            with open(hosts, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    found = re.search(rf"^(\S+)[ \t]+{re.escape(self.hostname)}$", line)
                    if found:
                        return found.group(1)
        return None

    def install_ssh_key(self):
        ssh_dir = Path.home() / ".ssh"
        pubkey_path = ssh_dir / "id_ed25519.pub"
        privkey_path = ssh_dir / "id_ed25519"
        ssh_dir.mkdir(mode=0o700, exist_ok=True)

        if not pubkey_path.exists() or not privkey_path.exists():
            _log("VPSManager", "No SSH key found - generating a new ed25519 key pair...", level="INFO")
            try:
                subprocess.run(
                    [
                        "ssh-keygen",
                        "-t",
                        "ed25519",
                        "-C",
                        f"{self.user}@{self.hostname}",
                        "-f",
                        str(privkey_path),
                        "-N",
                        "",
                    ],
                    check=True,
                )
                _log("VPSManager", f"SSH key generated: {pubkey_path}", level="INFO")
            except Exception as exc:
                _log("VPSManager", f"Failed to generate SSH key: {exc}", level="ERROR")
                return
        else:
            _log("VPSManager", f"Found existing SSH key: {pubkey_path}", level="INFO")

        if not self.user_pw:
            _log("VPSManager", "Password is required to install the SSH key.", level="ERROR")
            return

        target = f"{self.user}@{self.hostname}"
        _log("VPSManager", f"Installing SSH key to {target}...", level="INFO")
        try:
            result = subprocess.run(
                [
                    "sshpass",
                    "-p",
                    self.user_pw,
                    "ssh-copy-id",
                    "-o",
                    "StrictHostKeyChecking=no",
                    target,
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                _log("VPSManager", f"SSH key successfully installed to {target}", level="INFO")
            else:
                _log(
                    "VPSManager",
                    f"Failed to install SSH key. Output:\n{result.stdout}\n{result.stderr}",
                    level="WARNING",
                )
        except FileNotFoundError:
            _log("VPSManager", "ssh-copy-id or sshpass is not installed on this machine.", level="ERROR")
        except Exception as exc:
            _log("VPSManager", f"Unexpected error: {exc}", level="ERROR")

    def can_login_ssh(self, timeout: int = 5) -> bool:
        if not all([self.ip, self.user]):
            _log("VPSManager", "Missing SSH credentials (IP or username).", level="WARNING")
            return False

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            _log("VPSManager", f"Trying SSH connection to {self.user}@{self.ip} with key authentication...", level="INFO")
            ssh.connect(
                hostname=self.ip,
                username=self.user,
                timeout=timeout,
                banner_timeout=timeout,
                auth_timeout=timeout,
                allow_agent=True,
                look_for_keys=True,
            )
            _log("VPSManager", f"Successfully connected to {self.user}@{self.ip} using key authentication", level="INFO")
            ssh.close()
            return True
        except paramiko.AuthenticationException:
            _log("VPSManager", f"Key authentication failed for {self.user}@{self.ip}. Trying password login...", level="WARNING")
        except paramiko.SSHException as exc:
            if "No authentication methods available" in str(exc):
                _log("VPSManager", "Key login not available, will try password", level="WARNING")
            else:
                _log("VPSManager", f"SSH error: {exc}", level="WARNING")
                ssh.close()
                return False
        except Exception as exc:
            _log("VPSManager", f"Unexpected error: {exc}", level="ERROR")
            ssh.close()
            return False

        _log("VPSManager", f"Trying SSH connection to {self.user}@{self.ip} with password authentication...", level="INFO")
        if getattr(self, "user_pw", None):
            _log("VPSManager", f"Using password authentication for {self.user}@{self.ip}", level="INFO")
            try:
                ssh.connect(
                    hostname=self.ip,
                    username=self.user,
                    password=self.user_pw,
                    timeout=timeout,
                    banner_timeout=timeout,
                    auth_timeout=timeout,
                    allow_agent=False,
                    look_for_keys=False,
                )
                _log("VPSManager", f"Successfully connected to {self.user}@{self.ip} using password", level="INFO")
                try:
                    _log("VPSManager", f"Installing SSH key for {self.user}@{self.ip}...", level="INFO")
                    self.install_ssh_key()
                    _log("VPSManager", "SSH key installed successfully", level="INFO")
                except Exception as exc:
                    _log("VPSManager", f"Failed to install SSH key: {exc}", level="WARNING")
                ssh.close()
                return True
            except paramiko.AuthenticationException:
                _log("VPSManager", f"Password authentication failed for {self.user}@{self.ip}.", level="ERROR")
            except (paramiko.SSHException, socket.timeout) as exc:
                _log("VPSManager", f"SSH error while connecting with password: {exc}", level="WARNING")
            except Exception as exc:
                _log("VPSManager", f"Unexpected error during password login: {exc}", level="ERROR")
        else:
            _log("VPSManager", "No password provided; cannot fallback to password login.", level="WARNING")

        ssh.close()
        _log("VPSManager", f"SSH session to {self.ip} closed.", level="DEBUG")
        return False

    def fetch_vps_info(self):
        result = {"bucket": None, "coinmarketcap": None, "swap": "0"}
        if not self.ip or not self.user:
            _log("VPSManager", "Missing VPS IP or username.", level="WARNING")
            return result

        try:
            _log("VPSManager", f"Connecting to VPS {self.hostname} ({self.ip})...", level="INFO")
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.ip, username=self.user, password=self.user_pw, timeout=5)

            try:
                stdin, stdout, stderr = ssh.exec_command(
                    "swapon --show --noheadings --raw | awk '$1==\"/swapfile\" {print $3}'"
                )
                del stdin, stderr
                swap_size = stdout.read().decode().strip()
                result["swap"] = swap_size if swap_size else "0"
                _log("VPSManager", f"Swap size on VPS {self.hostname}: {result['swap']}", level="DEBUG")
            except Exception as exc:
                _log("VPSManager", f"Failed to get swap size on VPS {self.hostname}: {exc}", level="WARNING")

            sftp = ssh.open_sftp()
            try:
                remote_dirs = []
                for item in (self.remote_pbgui_dir, "software/pbgui", "pbgui"):
                    value = str(item or "").strip().rstrip("/")
                    if value and value not in remote_dirs:
                        remote_dirs.append(value)
                content = None
                for remote_dir in remote_dirs:
                    remote_path = f"{remote_dir}/pbgui.ini"
                    try:
                        with sftp.file(remote_path, mode="r") as config_file:
                            content = config_file.read().decode()
                        if content and remote_dir != self.remote_pbgui_dir:
                            self.remote_pbgui_dir = remote_dir
                        break
                    except FileNotFoundError:
                        continue
                    except Exception as exc:
                        _log(
                            "VPSManager",
                            f"Error reading file from VPS {self.hostname} ({self.ip}): {exc}",
                            level="ERROR",
                        )
                        break
                if not content:
                    _log(
                        "VPSManager",
                        f"pbgui.ini not found on VPS {self.hostname} ({self.ip}) in: {', '.join(remote_dirs)}",
                        level="ERROR",
                    )
            finally:
                sftp.close()
                ssh.close()

            if not content:
                return result

            config_data = configparser.ConfigParser()
            try:
                config_data.read_string(content)
            except Exception as exc:
                _log("VPSManager", f"Error parsing config file from VPS {self.hostname} ({self.ip}): {exc}", level="WARNING")
                return result

            if config_data.has_section("pbremote") and config_data.has_option("pbremote", "bucket"):
                result["bucket"] = config_data.get("pbremote", "bucket")
                _log("VPSManager", f"Successfully fetched PBRemote bucket from {self.hostname}", level="INFO")
            else:
                _log("VPSManager", f"'bucket' not found in [pbremote] section on VPS {self.hostname}", level="WARNING")

            if config_data.has_section("coinmarketcap") and config_data.has_option("coinmarketcap", "api_key"):
                result["coinmarketcap"] = config_data.get("coinmarketcap", "api_key")
                _log("VPSManager", f"Successfully fetched API key from {self.hostname}", level="INFO")
            else:
                _log("VPSManager", f"'api_key' not found in [coinmarketcap] section on VPS {self.hostname}", level="WARNING")
        except Exception as exc:
            _log("VPSManager", f"Error connecting to VPS {self.hostname} ({self.ip}): {exc}", level="ERROR")

        return result

    def fetch_ufw_settings(self, timeout: int = 5) -> tuple:
        allowed_ips = []
        fw_enabled = False
        if not all([self.ip, self.user, self.user_pw]):
            _log("VPSManager", "Missing SSH credentials (IP, username, or sudo password).", level="WARNING")
            return fw_enabled, ""

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            _log("VPSManager", f"Connecting to {self.user}@{self.ip} to fetch UFW settings...", level="INFO")
            ssh.connect(
                hostname=self.ip,
                username=self.user,
                password=self.user_pw,
                timeout=timeout,
                banner_timeout=timeout,
                auth_timeout=timeout,
                look_for_keys=False,
                allow_agent=False,
            )

            command = f"echo {shlex.quote(self.user_pw)} | sudo -S ufw status"
            stdin, stdout, stderr = ssh.exec_command(command, timeout=timeout)
            del stdin
            output = stdout.read().decode(errors="ignore")
            errors = stderr.read().decode(errors="ignore")
            errors = re.sub(r"\[sudo\] password for .*?:\s*", "", errors).strip()

            _log("VPSManager", f"Raw UFW output: {output}", level="DEBUG")
            if errors:
                _log("VPSManager", f"Raw errors from UFW command: {errors}", level="WARNING")

            if any(
                err in errors.lower()
                for err in [
                    "incorrect password",
                    "sorry, try again",
                    "no password was provided",
                    "a password is required",
                    "1 incorrect password attempt",
                ]
            ):
                _log("VPSManager", "Wrong sudo password provided.", level="ERROR")
                ssh.close()
                return fw_enabled, ""

            if re.search(r"Status:\s+active", output, re.IGNORECASE):
                fw_enabled = True
            else:
                _log("VPSManager", "Firewall is disabled!", level="WARNING")

            pattern = re.compile(r"^22/tcp\s+ALLOW\s+([0-9.:/A-Za-z]+)", re.IGNORECASE)
            for line in output.splitlines():
                line = line.strip()
                match = pattern.search(line)
                if match:
                    ip = match.group(1)
                    allowed_ips.append(ip)
                    if ip.lower() in ("anywhere", "anywhere (v6)", "0.0.0.0/0"):
                        _log("VPSManager", "SSH is open to any IP!", level="WARNING")

            _log("VPSManager", f"Firewall enabled: {fw_enabled}, Allowed SSH IPs: {allowed_ips}", level="INFO")
        except paramiko.AuthenticationException:
            _log("VPSManager", f"SSH authentication failed for {self.user}@{self.ip}.", level="ERROR")
        except (paramiko.SSHException, socket.timeout) as exc:
            _log("VPSManager", f"SSH connection error: {exc}", level="WARNING")
        except Exception as exc:
            _log("VPSManager", f"Unexpected error: {exc}", level="ERROR")
        finally:
            ssh.close()
            _log("VPSManager", f"SSH session to {self.ip} closed.", level="DEBUG")

        return fw_enabled, ",".join(allowed_ips)

    def is_vps_ssh_open(self):
        if not self.ip:
            return False
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.5)
        try:
            result = sock.connect_ex((self.ip, 22))
            return result == 0
        finally:
            sock.close()

    def fetch_package_status(self, timeout: int = 10) -> dict | None:
        result = {"upgrades": "N/A", "reboot": False}
        if not all([self.ip, self.user, self.user_pw]):
            return None

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(
                hostname=self.ip,
                username=self.user,
                password=self.user_pw,
                timeout=timeout,
                banner_timeout=timeout,
                auth_timeout=timeout,
                look_for_keys=False,
                allow_agent=False,
            )

            stdin, stdout, stderr = ssh.exec_command(
                "LANG=C sudo -S -p '' apt-get dist-upgrade -s",
                timeout=timeout,
                get_pty=True,
            )
            stdin.write(self.user_pw + "\n")
            stdin.flush()
            output = stdout.read().decode(errors="ignore")
            errors = stderr.read().decode(errors="ignore")
            combined = (output + "\n" + errors).strip()
            match = re.search(r"(\d+) upgraded", combined)
            if match:
                result["upgrades"] = int(match.group(1))

            stdin, stdout, stderr = ssh.exec_command(
                "test -f /var/run/reboot-required && echo yes || echo no",
                timeout=timeout,
            )
            del stdin, stderr
            result["reboot"] = stdout.read().decode(errors="ignore").strip().lower() == "yes"
            return result
        except Exception as exc:
            _log("VPSManager", f"Failed to fetch live package status for {self.hostname}: {exc}", level="WARNING")
            return None
        finally:
            ssh.close()

    def has_init_parameters(self):
        if self.ip and self.user and self.user_pw and self.root_pw and self.initial_root_pw:
            return True
        if self.ip and self.user and self.user_pw and self.private_key_user and self.private_key_file:
            return True
        if self.ip and self.user and self.user_pw and self.user_sudo and self.user_sudo_pw:
            return True
        return False

    def has_setup_parameters(self):
        return bool(self.hostname and self.user and self.user_pw and self.swap)

    def has_user_pw(self):
        return bool(self.user_pw)

    def is_initialized(self):
        return self.init_status == "successful"

    def init_event_handler(self, event):
        if dump := event.get("stdout"):
            self._append_task_log(dump, task_name="vps-init", fallback="vps-init", buffer_attr="init_log")

    def setup_event_handler(self, event):
        if dump := event.get("stdout"):
            self._append_task_log(dump, task_name="vps-setup", fallback="vps-setup", buffer_attr="setup_log")

    def update_event_handler(self, event):
        if dump := event.get("stdout"):
            self._append_task_log(dump, task_name=self.command, fallback="vps-update", buffer_attr="update_log")

    def remove_init_log(self):
        self._rotate_task_log(task_name="vps-init", fallback="vps-init")
        self._start_task_log(task_name="vps-init", fallback="vps-init")

    def remove_setup_log(self):
        self._rotate_task_log(task_name="vps-setup", fallback="vps-setup")
        self._start_task_log(task_name="vps-setup", fallback="vps-setup")

    def remove_update_log(self):
        self._rotate_task_log(task_name=self.command, fallback="vps-update")
        self._start_task_log(task_name=self.command, fallback="vps-update")

    def init_status_handler(self, status_data, runner_config):
        del runner_config
        self.init_status = status_data["status"]

    def setup_status_handler(self, status_data, runner_config):
        del runner_config
        self.setup_status = status_data["status"]

    def update_status_handler(self, status_data, runner_config):
        del runner_config
        self.update_status = status_data["status"]

    def init_finished(self, runner_config=None):
        del runner_config
        self.last_init = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save()
        shutil.rmtree(f"{self.path}/tmp", ignore_errors=True)

    def setup_finished(self, runner_config=None):
        del runner_config
        self.last_setup = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if str(self.setup_status or "") == "successful":
            _set_vps_monitor_enabled(self.hostname, enabled=True)
        self.save()
        shutil.rmtree(f"{self.path}/tmp", ignore_errors=True)

    def update_finished(self, runner_config=None):
        del runner_config
        self.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save()
        shutil.rmtree(f"{self.path}/tmp", ignore_errors=True)

    def fetch_log_finished(self, runner_config=None):
        del runner_config
        shutil.rmtree(f"{self.path}/tmp", ignore_errors=True)
        self.load_log()

    def load_log(self):
        if self.logfilename:
            log = Path(f"{self.path}/{self.logfilename}")
            if log.exists():
                with open(log, "rb") as handle:
                    handle.seek(0, 2)
                    file_size = handle.tell()
                    start_pos = max(file_size - self.logsize * 1024, 0)
                    handle.seek(start_pos)
                    self.logfile = handle.read().decode("utf-8", errors="ignore")

    def get_init_log_text(self):
        return strip_ansi(self.init_log)

    def get_setup_log_text(self):
        return strip_ansi(self.setup_log)

    def get_update_log_text(self):
        candidates = [
            self._task_log_path(self.command, "vps-update"),
            self._task_log_alias_path(self.command, "vps-update"),
        ]
        for path in candidates:
            try:
                if path.exists():
                    return strip_ansi(path.read_text(encoding="utf-8", errors="ignore"))
            except Exception:
                continue
        return strip_ansi(self.update_log)

    def save(self):
        if self.hostname:
            self.path = Path(f"{PBGDIR}/data/vpsmanager/hosts/{self.hostname}")
            self.path.mkdir(parents=True, exist_ok=True)
            file_path = f"{self.path}/{self.hostname}.json"
            # Never persist credentials or bootstrap secrets here. VPS passwords,
            # sudo credentials, and init private-key fields must stay session-only
            # in memory. Re-introducing them into host JSON would recreate the
            # security regression this module now guards against.
            config = {
                "_hostname": self.hostname,
                "ip": self.ip,
                "user": self.user,
                "swap": self.swap,
                "bucket": self.bucket,
                "coinmarketcap_api_key": self.coinmarketcap_api_key,
                "last_setup": self.last_setup,
                "last_init": self.last_init,
                "last_update": self.last_update,
                "setup_status": self.setup_status,
                "init_status": self.init_status,
                "update_status": self.update_status,
                "firewall": self.firewall,
                "firewall_ssh_port": self.firewall_ssh_port,
                "firewall_ssh_ips": self.firewall_ssh_ips,
                "command": self.command,
                "command_text": self.command_text,
                "command_run_id": self.command_run_id,
                "init_methode": self.init_methode,
                "remove_user": self.remove_user,
                "remote_pbgui_dir": self.remote_pbgui_dir,
            }
            with open(file_path, "w", encoding="utf-8") as handle:
                json.dump(config, handle, indent=4)

    def delete(self):
        vps_path = Path(f"{PBGDIR}/data/vpsmanager/hosts/{self.hostname}")
        shutil.rmtree(vps_path, ignore_errors=True)


class VPSManager:
    def __init__(self):
        self.vpss = []
        self.path = Path(f"{PBGDIR}/data/vpsmanager/hosts")
        self.privat_data_dir = None
        self.last_update = None
        self.command = "unknown"
        self.command_text = "unknown"
        self.update_status = None
        self.update_log = ""
        self.find_vps()
        self.load_hostname()
        self.load_master()

    def _task_log_path(self, task_name: str | None = None, fallback: str = "master-update-pb") -> Path:
        return _task_log_path(Path(f"{PBGDIR}/data/vpsmanager"), task_name or self.command, fallback)

    def _rotate_task_log(self, task_name: str | None = None, fallback: str = "master-update-pb") -> None:
        _rotate_task_log(self._task_log_path(task_name, fallback), _task_log_history_limit())

    def _append_task_log(self, dump: str, *, task_name: str | None = None, fallback: str, buffer_attr: str) -> None:
        log = self._task_log_path(task_name, fallback)
        with open(log, "a", encoding="utf-8") as logfile:
            logfile.write(dump)
        setattr(self, buffer_attr, getattr(self, buffer_attr) + dump)

    def _start_task_log(self, task_name: str | None = None, fallback: str = "master-update-pb") -> None:
        log = self._task_log_path(task_name, fallback)
        log.parent.mkdir(parents=True, exist_ok=True)
        header = _task_log_header(task_name=task_name or self.command, fallback=fallback, target="master")
        with open(log, "w", encoding="utf-8") as logfile:
            logfile.write(header)

    def update_event_handler(self, event):
        if dump := event.get("stdout"):
            self._append_task_log(dump, task_name=self.command, fallback="master-update-pb", buffer_attr="update_log")

    def update_status_handler(self, status_data, runner_config):
        del runner_config
        self.update_status = status_data["status"]

    def update_finished(self, runner_config=None):
        del runner_config
        self.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_master()
        shutil.rmtree(f"{PBGDIR}/data/vpsmanager/tmp", ignore_errors=True)

    def remove_update_log(self):
        self._rotate_task_log(task_name=self.command, fallback="master-update-pb")
        self._start_task_log(task_name=self.command, fallback="master-update-pb")

    def get_update_log_text(self):
        return strip_ansi(self.update_log)

    def list(self):
        return [vps.hostname for vps in self.vpss]

    def find_vps_by_hostname(self, hostname):
        for vps in self.vpss:
            if vps.hostname == hostname:
                return vps
        return None

    def find_vps(self):
        pattern = str(Path(f"{PBGDIR}/data/vpsmanager/hosts/*/*.json"))
        hosts = glob.glob(pattern, recursive=False)
        if hosts:
            for host in hosts:
                vps = VPS()
                vps.load(host)
                self.vpss.append(vps)
        if self.vpss:
            self.vpss.sort(key=lambda item: item.hostname)

    def add_vps(self):
        return VPS()

    def init_vps(self, vps: VPS, debug=False, auto_setup=False):
        vps.command = "vps-init"
        vps.command_text = "Initialize"
        vps.init_status = "starting"
        vps.setup_status = None
        vps.save()
        vps.privat_data_dir = _prepare_runner_private_data_dir(vps.path)
        vps.remove_init_log()
        vps.init_log = ""
        if debug:
            tags = "debug,all"
            verbosity = 3
        else:
            tags = None
            verbosity = 1

        def _on_init_finished(runner_config=None):
            vps.init_finished(runner_config)
            if auto_setup and getattr(vps, "init_status", None) == "successful":
                try:
                    self.setup_vps(vps, debug=debug)
                except Exception:
                    pass

        try:
            ansible_runner.run_async(
                playbook=str(PurePath(f"{PBGDIR}/vps-init.yml")),
                inventory=vps.hostname,
                extravars={
                    "hostname": vps.hostname,
                    "ip": vps.ip,
                    "initial_root_pw": vps.initial_root_pw if vps.init_methode == "root" else vps.user_sudo_pw if vps.init_methode == "password" else "",
                    "init_user": vps.private_key_user if vps.init_methode == "private_key" else vps.user_sudo if vps.init_methode == "password" else "root",
                    "privatel_key_file": vps.private_key_file,
                    "root_pw": vps.root_pw,
                    "user": vps.user,
                    "user_pw": vps.user_pw,
                    "remove_user": vps.remove_user,
                    "debug": debug,
                },
                quiet=True,
                envvars=_ansible_envvars(),
                tags=tags,
                verbosity=verbosity,
                private_data_dir=vps.privat_data_dir,
                event_handler=vps.init_event_handler,
                status_handler=vps.init_status_handler,
                finished_callback=_on_init_finished,
            )
        except Exception:
            shutil.rmtree(vps.privat_data_dir, ignore_errors=True)
            raise

    def setup_vps(self, vps: VPS, debug=False, extra_vars=None):
        vps.command = "vps-setup"
        vps.command_text = "Setup VPS"
        vps.setup_status = "starting"
        vps.save()
        vps.privat_data_dir = _prepare_runner_private_data_dir(vps.path)
        vps.remove_setup_log()
        vps.setup_log = ""
        if debug:
            tags = "debug,all"
            verbosity = 3
        else:
            tags = None
            verbosity = 1
        ansible_extravars = {
            "hostname": vps.hostname,
            "user": vps.user,
            "user_pw": vps.user_pw,
            "swap_size": vps.swap,
            "bucket": str(vps.bucket or ""),
            "coinmarketcap_api_key": str(vps.coinmarketcap_api_key or ""),
            "firewall": vps.firewall,
            "firewall_ssh_port": vps.firewall_ssh_port,
            "firewall_ssh_ips": vps.firewall_ssh_ips.split(","),
            "debug": debug,
            "install_dir": _install_dir_from_remote_pbgui_dir(vps.remote_pbgui_dir, vps.user),
            "vps_logging_services": [],
        }
        if extra_vars:
            ansible_extravars.update(extra_vars)
        try:
            ansible_runner.run_async(
                playbook=str(PurePath(f"{PBGDIR}/vps-setup.yml")),
                inventory=vps.hostname,
                extravars=ansible_extravars,
                quiet=True,
                envvars=_ansible_envvars(),
                tags=tags,
                verbosity=verbosity,
                private_data_dir=vps.privat_data_dir,
                event_handler=vps.setup_event_handler,
                status_handler=vps.setup_status_handler,
                finished_callback=vps.setup_finished,
            )
        except Exception:
            shutil.rmtree(vps.privat_data_dir, ignore_errors=True)
            raise

    def update_vps(self, vps: VPS, debug=False, extra_vars=None):
        vps.update_status = "starting"
        vps.last_update = None
        vps.command_run_id = f"run-{int(datetime.now().timestamp() * 1000)}"
        vps.save()
        vps.privat_data_dir = _prepare_runner_private_data_dir(vps.path)
        vps.remove_update_log()
        vps.update_log = ""
        if debug:
            tags = "debug,all"
            verbosity = 3
        else:
            tags = None
            verbosity = 1

        ansible_extravars = {
            "hostname": vps.hostname,
            "user": vps.user,
            "user_pw": vps.user_pw,
            "swap_size": vps.swap,
            "bucket": str(vps.bucket or ""),
            "coinmarketcap_api_key": str(vps.coinmarketcap_api_key or ""),
            "firewall": vps.firewall,
            "firewall_ssh_port": vps.firewall_ssh_port,
            "firewall_ssh_ips": vps.firewall_ssh_ips.split(","),
            "reboot": vps.reboot,
            "debug": debug,
            "install_dir": _install_dir_from_remote_pbgui_dir(vps.remote_pbgui_dir, vps.user),
        }
        if extra_vars:
            ansible_extravars.update(extra_vars)

        reboot_requested = _coerce_bool(
            ansible_extravars.get("reboot_requested", ansible_extravars.get("reboot"))
        )
        ansible_extravars["reboot"] = reboot_requested
        ansible_extravars["reboot_requested"] = reboot_requested

        try:
            ansible_runner.run_async(
                playbook=str(PurePath(f"{PBGDIR}/{vps.command}.yml")),
                inventory=vps.hostname,
                extravars=ansible_extravars,
                quiet=True,
                envvars=_ansible_envvars(),
                tags=tags,
                verbosity=verbosity,
                private_data_dir=vps.privat_data_dir,
                event_handler=vps.update_event_handler,
                status_handler=vps.update_status_handler,
                finished_callback=vps.update_finished,
            )
        except Exception:
            shutil.rmtree(vps.privat_data_dir, ignore_errors=True)
            raise

    def fetch_log(self, vps: VPS, debug=False):
        vps.save()
        vps.privat_data_dir = _prepare_runner_private_data_dir(vps.path)
        if debug:
            tags = "debug,all"
            verbosity = 3
        else:
            tags = None
            verbosity = 1
        try:
            ansible_runner.run(
                playbook=str(PurePath(f"{PBGDIR}/{vps.command}.yml")),
                inventory=vps.hostname,
                extravars={
                    "hostname": vps.hostname,
                    "user": vps.user,
                    "vps_dir": str(vps.path) + "/" + str(PurePath(vps.logfilename).parent),
                    "logfile": vps.logfilename,
                    "debug": debug,
                    "install_dir": _install_dir_from_remote_pbgui_dir(vps.remote_pbgui_dir, vps.user),
                },
                quiet=True,
                envvars=_ansible_envvars(),
                tags=tags,
                verbosity=verbosity,
                private_data_dir=vps.privat_data_dir,
                finished_callback=vps.fetch_log_finished,
            )
        except Exception:
            shutil.rmtree(vps.privat_data_dir, ignore_errors=True)
            raise

    def update_master(self, debug=False, sudo_pw=None, extra_vars=None):
        self.update_status = None
        self.privat_data_dir = Path(f"{PBGDIR}/data/vpsmanager/tmp")
        self.privat_data_dir.mkdir(parents=True, exist_ok=True)
        self.remove_update_log()
        self.update_log = ""
        if debug:
            tags = "debug,all"
            verbosity = 3
        else:
            tags = None
            verbosity = 1

        ansible_extravars = {
            "pbgdir": str(PBGDIR),
            "pb7dir": str(PB7DIR),
            "pb7venv": str(PurePath(PB7VENV).parents[1]),
            "user_pw": sudo_pw,
            "debug": debug,
        }
        if extra_vars:
            ansible_extravars.update(extra_vars)

        ansible_runner.run_async(
            playbook=str(PurePath(f"{PBGDIR}/{self.command}.yml")),
            extravars=ansible_extravars,
            quiet=True,
            envvars=_ansible_envvars(),
            tags=tags,
            verbosity=verbosity,
            private_data_dir=self.privat_data_dir,
            event_handler=self.update_event_handler,
            status_handler=self.update_status_handler,
            finished_callback=self.update_finished,
        )

    def load_hostname(self):
        self.hostname = load_ini("main", "pbname")
        if not self.hostname:
            self.hostname = platform.node()

    def load_master(self):
        self.path = Path(f"{PBGDIR}/data/vpsmanager")
        file_path = f"{self.path}/{self.hostname}.json"
        if Path(file_path).exists():
            with open(file_path, "r", encoding="utf-8") as handle:
                config = json.load(handle)
            if "last_update" in config:
                self.last_update = config["last_update"]
            if "update_status" in config:
                self.update_status = config["update_status"]
            if "command" in config:
                self.command = config["command"]
            if "command_text" in config:
                self.command_text = config["command_text"]

    def save_master(self):
        self.path = Path(f"{PBGDIR}/data/vpsmanager")
        file_path = f"{self.path}/{self.hostname}.json"
        config = {
            "last_update": self.last_update,
            "update_status": self.update_status,
            "command": self.command,
            "command_text": self.command_text,
        }
        with open(file_path, "w", encoding="utf-8") as handle:
            json.dump(config, handle, indent=4)
