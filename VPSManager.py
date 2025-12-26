import configparser
import streamlit as st
from time import sleep
import platform
import json
from pathlib import Path, PurePath
from datetime import datetime
import glob
import ansible_runner
import re
import getpass
import shutil
import socket
import paramiko
import subprocess
import re
import shlex
from pbgui_purefunc import pbdir, pbvenv, pb7dir, pb7venv, load_ini

PBGDIR = Path.cwd()
PBDIR = pbdir()
PB7DIR = pb7dir()
PBVENV = pbvenv()
PB7VENV = pb7venv()

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
        self.init_methode = "root"  # root, password, private_key
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
        self.install_pb6 = True

    @property
    def hostname(self):
        return self._hostname

    @hostname.setter
    def hostname(self, new_hostanme):
        self._hostname = new_hostanme
        self.path = Path(f'{PBGDIR}/data/vpsmanager/hosts/{self.hostname}')

    def load(self, file):
        with open(file, 'r') as f:
            config = json.load(f)
            if "_hostname" in config:
                self._hostname = config["_hostname"]
                self.path = Path(f'{PBGDIR}/data/vpsmanager/hosts/{self.hostname}')
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
            if "install_pb6" in config:
                self.install_pb6 = config["install_pb6"]

    def is_vps_in_hosts(self):
        # open /etc/hosts and check if the ip and hostname is in there
        hosts = Path('/etc/hosts')
        if hosts.exists():
            with open(hosts, 'r') as f:
                for line in f:
                    found = re.search(f'^{self.ip}[ \t]+{self.hostname}$', line)
                    if found:
                        return True
        return False
    
    def fetch_vps_ip_from_hosts(self):
        """
        Open /etc/hosts and get the IP for self.hostname,
        ignoring commented lines.
        """
        hosts = Path('/etc/hosts')
        if hosts.exists():
            with open(hosts, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue  # skip comments and empty lines
                    # Match IP and hostname
                    found = re.search(rf'^(\S+)[ \t]+{re.escape(self.hostname)}$', line)
                    if found:
                        return found.group(1)
        return None

    def install_ssh_key(self):
        """
        Installs the local SSH public key on the remote server using ssh-copy-id.
        If no SSH key exists, automatically generates one (ed25519).
        Uses self.user, self.hostname, and self.user_pw (must be set).
        """

        ssh_dir = Path.home() / ".ssh"
        pubkey_path = ssh_dir / "id_ed25519.pub"
        privkey_path = ssh_dir / "id_ed25519"

        # Ensure ~/.ssh exists
        ssh_dir.mkdir(mode=0o700, exist_ok=True)

        # Generate SSH key if missing
        if not pubkey_path.exists() or not privkey_path.exists():
            print("🪪 No SSH key found — generating a new ed25519 key pair...")
            try:
                subprocess.run([
                    "ssh-keygen",
                    "-t", "ed25519",
                    "-C", f"{self.user}@{self.hostname}",
                    "-f", str(privkey_path),
                    "-N", ""
                ], check=True)
                print(f"✅ SSH key generated: {pubkey_path}")
            except Exception as e:
                print(f"💥 Failed to generate SSH key: {e}")
                return
        else:
            print(f"🔑 Found existing SSH key: {pubkey_path}")

        # Ensure password provided
        if not self.user_pw:
            print("❌ Password is required to install the SSH key.")
            return

        target = f"{self.user}@{self.hostname}"
        print(f"🔌 Installing SSH key to {target}...")

        try:
            cmd = ["sshpass", "-p", self.user_pw, "ssh-copy-id", "-o", "StrictHostKeyChecking=no", target]
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                print(f"✅ SSH key successfully installed to {target}")
            else:
                print(f"⚠️ Failed to install SSH key. Output:\n{result.stdout}\n{result.stderr}")

        except FileNotFoundError:
            print("❌ ssh-copy-id or sshpass is not installed on this machine.")
        except Exception as e:
            print(f"💥 Unexpected error: {e}")

    def can_login_ssh(self, timeout: int = 5) -> bool:
        """
        Attempt SSH login using key authentication first, then fallback to password authentication.
        Installs SSH key if key login failed and password login succeeds.

        Returns:
            bool: True if login succeeds, False otherwise.
        """
        if not all([self.ip, self.user]):
            print("⚠️ Missing SSH credentials (IP or username).")
            return False

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # --- Attempt key authentication first ---
            print(f"🔌 Trying SSH connection to {self.user}@{self.ip} with key authentication...")
            ssh.connect(
                hostname=self.ip,
                username=self.user,
                timeout=timeout,
                banner_timeout=timeout,
                auth_timeout=timeout,
                allow_agent=True,
                look_for_keys=True,
            )
            print(f"✅ Successfully connected to {self.user}@{self.ip} using key authentication")
            ssh.close()
            return True

        except paramiko.AuthenticationException:
            print(f"⚠️ Key authentication failed for {self.user}@{self.ip}. Trying password login...")

        except paramiko.SSHException as e:
            # only skip/continue if key login not available
            if "No authentication methods available" in str(e):
                print("⚠️ Key login not available, will try password")
            else:
                print(f"⚠️ SSH error: {e}")
                ssh.close()
                return False
        except Exception as e:
            print(f"💥 Unexpected error: {e}")
            ssh.close()
            return False

        # --- Password login fallback ---
        print(f"🔌 Trying SSH connection to {self.user}@{self.ip} with password authentication...")
        if getattr(self, "user_pw", None):
            print(f"🔑 Using password authentication for {self.user}@{self.ip}")
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
                print(f"✅ Successfully connected to {self.user}@{self.ip} using password")

                # Install SSH key if key login failed
                try:
                    print(f"🔑 Installing SSH key for {self.user}@{self.ip}...")
                    self.install_ssh_key()
                    print(f"✅ SSH key installed successfully")
                except Exception as e:
                    print(f"⚠️ Failed to install SSH key: {e}")

                ssh.close()
                return True

            except paramiko.AuthenticationException:
                print(f"❌ Password authentication failed for {self.user}@{self.ip}.")
            except (paramiko.SSHException, socket.timeout) as e:
                print(f"⚠️ SSH error while connecting with password: {e}")
            except Exception as e:
                print(f"💥 Unexpected error during password login: {e}")

        else:
            print("⚠️ No password provided; cannot fallback to password login.")

        ssh.close()
        print(f"🔒 SSH session to {self.ip} closed.\n")
        return False

    def fetch_vps_info(self):
        """
        Fetch information from the VPS, including:
        - CoinMarketCap API key
        - Whether PB6 is installed (pbdir exists in [main] section)
        - Swap size in human-readable form (e.g., 512M, 2G)

        Returns:
            dict: {
                "pb6": bool,
                "coinmarketcap": str | None,
                "swap": str
            }
        """
        result = {"pb6": False, "coinmarketcap": None, "swap": "0"}

        if not self.ip or not self.user:
            print("⚠️ Missing VPS IP or username.")
            return result

        try:
            print(f"🔹 Connecting to VPS {self.hostname} ({self.ip})...")
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.ip, username=self.user, password=self.user_pw, timeout=5)

            # Fetch swap size (human-readable)
            try:
                stdin, stdout, stderr = ssh.exec_command(
                    "swapon --show --noheadings --raw | awk '$1==\"/swapfile\" {print $3}'"
                )
                swap_size = stdout.read().decode().strip()
                result["swap"] = swap_size if swap_size else "0"
                print(f"💾 Swap size on VPS {self.hostname}: {result['swap']}")
            except Exception as e:
                print(f"⚠️ Failed to get swap size on VPS {self.hostname}: {e}")

            # Fetch and parse pbgui.ini
            sftp = ssh.open_sftp()
            remote_path = 'software/pbgui/pbgui.ini'

            content = None
            try:
                with sftp.file(remote_path, mode='r') as config_file:
                    content = config_file.read().decode()
            except FileNotFoundError:
                print(f"❌ File not found on VPS {self.hostname} ({self.ip}): {remote_path}")
            except Exception as e:
                print(f"❌ Error reading file from VPS {self.hostname} ({self.ip}): {e}")
            finally:
                sftp.close()
                ssh.close()

            if not content:
                return result

            # Parse the INI content
            config_data = configparser.ConfigParser()
            try:
                config_data.read_string(content)
            except Exception as e:
                print(f"⚠️ Error parsing config file from VPS {self.hostname} ({self.ip}): {e}")
                return result

            # Check for CoinMarketCap API key
            if config_data.has_section("coinmarketcap") and config_data.has_option("coinmarketcap", "api_key"):
                result["coinmarketcap"] = config_data.get("coinmarketcap", "api_key")
                print(f"✅ Successfully fetched API key from {self.hostname} {result['coinmarketcap']}")
            else:
                print(f"⚠️ 'api_key' not found in [coinmarketcap] section on VPS {self.hostname}")

            # Check if PB6 is installed
            if config_data.has_section("main") and config_data.has_option("main", "pbdir"):
                pbdir = config_data.get("main", "pbdir").strip()
                if pbdir:
                    result["pb6"] = True
                    print(f"✅ PB6 detected on VPS {self.hostname}")
            else:
                print(f"ℹ️ PB6 not detected on VPS {self.hostname}")

        except Exception as e:
            print(f"❌ Error connecting to VPS {self.hostname} ({self.ip}): {e}")

        return result

    def fetch_ufw_settings(self, timeout: int = 5) -> tuple:
        """
        Fetch UFW settings via SSH.

        Returns:
            tuple:
                fw_enabled (bool): True if firewall is active, False if inactive.
                allowed_ips (str): Comma-separated list of allowed SSH IPs.
        """
        allowed_ips = []
        fw_enabled = False

        if not all([self.ip, self.user, self.user_pw]):
            print("⚠️ Missing SSH credentials (IP, username, or sudo password).")
            return fw_enabled, ""

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            print(f"🔌 Connecting to {self.user}@{self.ip} to fetch UFW settings...")

            ssh.connect(
                hostname=self.ip,
                username=self.user,
                password=self.user_pw,
                timeout=timeout,          # TCP connection timeout
                banner_timeout=timeout,   # SSH banner wait timeout
                auth_timeout=timeout,     # authentication timeout
                look_for_keys=False,      # skip local key lookup
                allow_agent=False,        # skip SSH agent
            )

            # Non-interactive sudo (prevents hanging)
            command = f"echo {shlex.quote(self.user_pw)} | sudo -S ufw status"
            stdin, stdout, stderr = ssh.exec_command(command, timeout=timeout)

            output = stdout.read().decode(errors="ignore")
            errors = stderr.read().decode(errors="ignore")
            # Remove harmless sudo prompt
            errors = re.sub(r"\[sudo\] password for .*?:\s*", "", errors).strip()

            print("📝 Raw UFW output:")
            print(output)
            if errors:
                print("⚠️ Raw errors from UFW command:")
                print(errors)

            # Detect wrong sudo password
            if any(err in errors.lower() for err in [
                "incorrect password",
                "sorry, try again",
                "no password was provided",
                "a password is required",
                "1 incorrect password attempt",
            ]):
                print("❌ Wrong sudo password provided.")
                ssh.close()
                return fw_enabled, ""

            # Check if firewall is active
            if re.search(r"Status:\s+active", output, re.IGNORECASE):
                fw_enabled = True
            else:
                print("⚠️ Firewall is disabled!")

            # Detect allowed SSH IPs (supports Anywhere and IPv6)
            pattern = re.compile(r"^22/tcp\s+ALLOW\s+([0-9.:/A-Za-z]+)", re.IGNORECASE)
            for line in output.splitlines():
                line = line.strip()
                match = pattern.search(line)
                if match:
                    ip = match.group(1)
                    allowed_ips.append(ip)
                    if ip.lower() in ("anywhere", "anywhere (v6)", "0.0.0.0/0"):
                        print("⚠️ SSH is open to any IP!")

            print(f"✅ Firewall enabled: {fw_enabled}")
            print(f"✅ Allowed SSH IPs: {allowed_ips}")

        except paramiko.AuthenticationException:
            print(f"❌ SSH authentication failed for {self.user}@{self.ip}.")
        except (paramiko.SSHException, socket.timeout) as e:
            print(f"⚠️ SSH connection error: {e}")
        except Exception as e:
            print(f"💥 Unexpected error: {e}")
        finally:
            ssh.close()
            print(f"🔒 SSH session to {self.ip} closed.\n")

        return fw_enabled, ",".join(allowed_ips)

    def is_vps_ssh_open(self):
        if not self.ip:
            return False
        # Test if ssh port open
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # set timeout to 0.5s
        sock.settimeout(0.5)
        result = sock.connect_ex((self.ip, 22))
        if result == 0:
            return True
        else:
            return False
    
    def has_init_parameters(self):
        if self.ip and self.user and self.user_pw and self.root_pw and self.initial_root_pw:
            return True
        elif self.ip and self.user and self.user_pw and self.private_key_user and self.private_key_file:
            return True
        elif self.ip and self.user and self.user_pw and self.user_sudo and self.user_sudo_pw:
            return True
        else:
            return False
    
    def has_setup_parameters(self):
        if self.hostname and self.user and self.user_pw and self.swap and self.bucket and self.coinmarketcap_api_key:
            return True
        else:
            return False
    
    def has_user_pw(self):
        if self.user_pw:
            return True
        else:
            return False

    def is_initialized(self):
        if self.init_status == "successful":
            return True
        else:
            return False

    @st.fragment(run_every=1)
    def view_init_status(self):
        st.text(f'Init Status: {self.init_status}')

    @st.fragment(run_every=1)
    def view_setup_status(self):
        st.text(f'Setup Status: {self.setup_status}')
    
    @st.fragment(run_every=1)
    def view_update_status(self):
        st.text(f'Update Status: {self.update_status}')

    @st.fragment(run_every=1)
    def view_init_log(self):
        ansi = self.init_log
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        result = ansi_escape.sub("", ansi)
        st.code(result, language="coffeescript")

    @st.fragment(run_every=1)
    def view_setup_log(self):
        ansi = self.setup_log
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        result = ansi_escape.sub("", ansi)
        st.code(result, language="coffeescript")
    
    @st.fragment(run_every=1)
    def view_update_log(self):
        ansi = self.update_log
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        result = ansi_escape.sub("", ansi)
        st.code(result, language="coffeescript")

    def init_event_handler(self, event):
        log = Path(f'{self.path}/vps_init.log')
        if (dump := event.get("stdout")):
            with open(log, "a") as logfile:
                logfile.write(dump)
            self.init_log = self.init_log + dump
    
    def setup_event_handler(self, event):
        log = Path(f'{self.path}/vps_setup.log')
        if (dump := event.get("stdout")):
            with open(log, "a") as logfile:
                logfile.write(dump)
            self.setup_log = self.setup_log + dump
    
    def update_event_handler(self, event):
        log = Path(f'{self.path}/vps_update.log')
        if (dump := event.get("stdout")):
            with open(log, "a") as logfile:
                logfile.write(dump)
            self.update_log = self.update_log + dump

    def remove_init_log(self):
        log = Path(f'{self.path}/vps_init.log')
        if log.exists():
            log.unlink()
    
    def remove_setup_log(self):
        log = Path(f'{self.path}/vps_setup.log')
        if log.exists():
            log.unlink()
    
    def remove_update_log(self):
        log = Path(f'{self.path}/vps_update.log')
        if log.exists():
            log.unlink()

    def init_status_handler(self, status_data, runner_config):
        self.init_status = status_data["status"]

    def setup_status_handler(self, status_data, runner_config):
        self.setup_status = status_data["status"]
    
    def update_status_handler(self, status_data, runner_config):
        self.update_status = status_data["status"]

    def init_finished(self, runner_config=None):
        self.last_init = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save()
        shutil.rmtree(f'{self.path}/tmp', ignore_errors=True)

    def setup_finished(self, runner_config=None):
        self.last_setup = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save()
        shutil.rmtree(f'{self.path}/tmp', ignore_errors=True)
    
    def update_finished(self, runner_config=None):
        self.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save()
        shutil.rmtree(f'{self.path}/tmp', ignore_errors=True)
    
    def fetch_log_finished(self, runner_config=None):
        shutil.rmtree(f'{self.path}/tmp', ignore_errors=True)
        self.load_log()

    def load_log(self):
        if self.logfilename:
            log = Path(f'{self.path}/{self.logfilename}')
            if log.exists():
                # Open the file in binary mode to handle raw bytes
                with open(log, 'rb') as f:
                    # Move the pointer to the last log_size KB (100 * 1024 bytes)
                    f.seek(0, 2)  # Move to the end of the file
                    file_size = f.tell()
                    # Ensure that we don't try to read more than the file size
                    start_pos = max(file_size - self.logsize * 1024, 0)
                    f.seek(start_pos)
                    # Read the last 100 KB (or less if the file is smaller)
                    self.logfile = f.read().decode('utf-8', errors='ignore')  # Decode and ignore errors

    def save(self):
        if self.hostname:
            self.path = Path(f'{PBGDIR}/data/vpsmanager/hosts/{self.hostname}')
            self.path.mkdir(parents=True, exist_ok=True)
            self.privat_data_dir = Path(f'{self.path}/tmp')
            self.privat_data_dir.mkdir(parents=True, exist_ok=True)
            file = f'{self.path}/{self.hostname}.json'
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
                "install_pb6": self.install_pb6
            }
            with open(file, "w", encoding='utf-8') as f:
                json.dump(config, f, indent=4)
    
    def delete(self):
        vps_path = Path(f'{PBGDIR}/data/vpsmanager/hosts/{self.hostname}')
        shutil.rmtree(vps_path, ignore_errors=True)
    
class VPSManager:
    def __init__(self):
        self.vpss = []
        self.path = Path(f'{PBGDIR}/data/vpsmanager/hosts')
        self.privat_data_dir = None
        self.last_update = None
        self.command = "unknown"
        self.command_text = "unknown"
        self.update_status = None
        self.update_log = ""
        self.find_vps()
        self.load_hostname()
        self.load_master()
    
    @st.fragment(run_every=1)
    def view_update_status(self):
        st.text(f'Update Status: {self.update_status}')

    @st.fragment(run_every=1)
    def view_update_log(self):
        ansi = self.update_log
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        result = ansi_escape.sub("", ansi)
        st.code(result, language="coffeescript")

    def update_event_handler(self, event):
        log = Path(f'{PBGDIR}/data/vpsmanager/vps_update.log')
        if (dump := event.get("stdout")):
            with open(log, "a") as logfile:
                logfile.write(dump)
            self.update_log = self.update_log + dump

    def update_status_handler(self, status_data, runner_config):
        self.update_status = status_data["status"]

    def update_finished(self, runner_config=None):
        self.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_master()
        shutil.rmtree(f'{PBGDIR}/data/vpsmanager/tmp', ignore_errors=True)

    def remove_update_log(self):
        log = Path(f'{PBGDIR}/data/vpsmanager/vps_update.log')
        if log.exists():
            log.unlink()

    def list(self):
        return list(map(lambda x: x.hostname, self.vpss))

    def find_vps_by_hostname(self, hostname):
        for vps in self.vpss:
            if vps.hostname == hostname:
                return vps
        return None

    def find_vps(self):
        p = str(Path(f'{PBGDIR}/data/vpsmanager/hosts/*/*.json'))
        hosts = glob.glob(p, recursive=False)
        if hosts:
            for host in hosts:
                v = VPS()
                # v.path = Path(host)
                v.load(host)
                self.vpss.append(v)
        # sort vpss by hostname
        if self.vpss:
            self.vpss.sort(key=lambda x: x.hostname)

    def add_vps(self):
        return VPS()

    def init_vps(self, vps : VPS, debug = False):
        vps.setup_status = None
        vps.save()
        vps.remove_init_log()
        vps.init_log = ""
        if debug:
            tags = "debug,all"
            verbosity = 3
        else:
            tags = None
            verbosity = 1
        ansible_runner.run_async(
            playbook=str(PurePath(f'{PBGDIR}/vps-init.yml')),
            inventory=vps.hostname,
            extravars={
                'hostname': vps.hostname,
                'ip': vps.ip,
                'initial_root_pw': vps.initial_root_pw if vps.init_methode == "root" else vps.user_sudo_pw if vps.init_methode == "password" else "",
                'init_user': vps.private_key_user if vps.init_methode == "private_key" else vps.user_sudo if vps.init_methode == "password" else "root",
                'privatel_key_file': vps.private_key_file,
                'root_pw': vps.root_pw,
                'user': vps.user,
                'user_pw': vps.user_pw,
                'remove_user': vps.remove_user,
                'debug': debug
            },
            quiet=True,
            tags=tags,
            verbosity=verbosity,
            private_data_dir=vps.privat_data_dir,
            event_handler=vps.init_event_handler,
            status_handler=vps.init_status_handler,
            finished_callback=vps.init_finished
        )
    
    def setup_vps(self, vps : VPS, debug = False):
        vps.save()
        vps.remove_setup_log()
        vps.setup_log = ""
        if debug:
            tags = "debug,all"
            verbosity = 3
        else:
            tags = None
            verbosity = 1
        ansible_runner.run_async(
            playbook=str(PurePath(f'{PBGDIR}/vps-setup.yml')),
            inventory=vps.hostname,
            extravars={
            'hostname': vps.hostname,
            'user': vps.user,
            'user_pw': vps.user_pw,
            'swap_size': vps.swap,
            'bucket': vps.bucket,
            'coinmarketcap_api_key': vps.coinmarketcap_api_key,
            'install_pb6': vps.install_pb6,
            'firewall': vps.firewall,
            'firewall_ssh_port': vps.firewall_ssh_port,
            'firewall_ssh_ips': vps.firewall_ssh_ips.split(','),
            'debug': debug
            },
            quiet=True,
            tags=tags,
            verbosity=verbosity,
            private_data_dir=vps.privat_data_dir,
            event_handler=vps.setup_event_handler,
            status_handler=vps.setup_status_handler,
            finished_callback=vps.setup_finished
        )

    def update_vps(self, vps : VPS, debug = False, extra_vars = None):
        vps.update_status = None
        vps.save()
        vps.remove_update_log()
        vps.update_log = ""
        if debug:
            tags = "debug,all"
            verbosity = 3
        else:
            tags = None
            verbosity = 1
        
        ansible_extravars = {
            'hostname': vps.hostname,
            'user': vps.user,
            'user_pw': vps.user_pw,
            'swap_size': vps.swap,
            'coinmarketcap_api_key': vps.coinmarketcap_api_key,
            'firewall': vps.firewall,
            'firewall_ssh_port': vps.firewall_ssh_port,
            'firewall_ssh_ips': vps.firewall_ssh_ips.split(','),
            'reboot': vps.reboot,
            'debug': debug
        }
        
        # Merge extra_vars if provided
        if extra_vars:
            ansible_extravars.update(extra_vars)
        
        ansible_runner.run_async(
            playbook=str(PurePath(f'{PBGDIR}/{vps.command}.yml')),
            inventory=vps.hostname,
            extravars=ansible_extravars,
            quiet=True,
            tags=tags,
            verbosity=verbosity,
            private_data_dir=vps.privat_data_dir,
            event_handler=vps.update_event_handler,
            status_handler=vps.update_status_handler,
            finished_callback=vps.update_finished
        )

    def fetch_log(self, vps : VPS, debug = False):
        # vps.update_status = None
        vps.save()
        # vps.remove_update_log()
        # vps.update_log = ""
        if debug:
            tags = "debug,all"
            verbosity = 3
        else:
            tags = None
            verbosity = 1
        ansible_runner.run(
            playbook=str(PurePath(f'{PBGDIR}/{vps.command}.yml')),
            inventory=vps.hostname,
            extravars={
                'hostname': vps.hostname,
                'user': vps.user,
                'vps_dir': str(vps.path) + "/" + str(PurePath(vps.logfilename).parent),
                'logfile': vps.logfilename,
                'debug': debug
            },
            quiet=True,
            tags=tags,
            verbosity=verbosity,
            private_data_dir=vps.privat_data_dir,
            # event_handler=vps.update_event_handler,
            # status_handler=vps.update_status_handler,
            finished_callback=vps.fetch_log_finished
        )

    def update_master(self, debug = False, sudo_pw = None, extra_vars = None):
        self.update_status = None
        self.privat_data_dir = Path(f'{PBGDIR}/data/vpsmanager/tmp')
        self.privat_data_dir.mkdir(parents=True, exist_ok=True)
        self.remove_update_log()
        self.update_log = ""
        if debug:
            tags = "debug,all"
            verbosity = 3
        else:
            tags = None
            verbosity = 1
        
        # Build extravars - start with defaults
        ansible_extravars = {
            'pbgdir': str(PBGDIR),
            'pb6dir': str(PBDIR),
            'pb7dir': str(PB7DIR),
            'pb7venv': str(PurePath(PB7VENV).parents[1]),
            'user_pw': sudo_pw,
            'debug': debug
        }
        
        # Merge in any additional extra_vars
        if extra_vars:
            ansible_extravars.update(extra_vars)
        
        ansible_runner.run_async(
            playbook=str(PurePath(f'{PBGDIR}/{self.command}.yml')),
            extravars=ansible_extravars,
            quiet=True,
            tags=tags,
            verbosity=verbosity,
            private_data_dir=self.privat_data_dir,
            event_handler=self.update_event_handler,
            status_handler=self.update_status_handler,
            finished_callback=self.update_finished
        )

    def load_hostname(self):
        self.hostname = load_ini("main", "pbname")        
        if not self.hostname:
            self.hostname = platform.node()

    def load_master(self):
        self.path = Path(f'{PBGDIR}/data/vpsmanager')
        file = f'{self.path}/{self.hostname}.json'
        if Path(file).exists():
            with open(file, 'r') as f:
                config = json.load(f)
                if "last_update" in config:
                    self.last_update = config["last_update"]
                if "update_status" in config:
                    self.update_status = config["update_status"]
                if "command" in config:
                    self.command = config["command"]
                if "command_text" in config:
                    self.command_text = config["command_text"]

    def save_master(self):
        self.path = Path(f'{PBGDIR}/data/vpsmanager')
        file = f'{self.path}/{self.hostname}.json'
        config = {
            "last_update": self.last_update,
            "update_status": self.update_status,
            "command": self.command,
            "command_text": self.command_text
        }
        with open(file, "w", encoding='utf-8') as f:
            json.dump(config, f, indent=4)

def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
