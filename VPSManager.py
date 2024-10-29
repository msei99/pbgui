import streamlit as st
from time import sleep
import json
from pathlib import Path, PurePath
from datetime import datetime
import glob
import ansible_runner
import re
import getpass
import shutil
import socket
import shlex


PBGDIR = Path.cwd()

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
        self.swap = None
        self.last_init = None
        self.last_setup = None
        self.init_status = None
        self.setup_status = None
        self.init_log = ""
        self.setup_log = ""
        self.bucket = None
        self.coinmarketcap_api_key = None
    
    @property
    def hostname(self):
        return self._hostname

    @hostname.setter
    def hostname(self, new_hostanme):
        self._hostname = new_hostanme
        self.path = Path(f'{PBGDIR}/data/vpsmanager/hosts/{self.hostname}')

    def load(self):
        with open(self.path, 'r') as f:
            config = json.load(f)
            if "_hostname" in config:
                self._hostname = config["_hostname"]
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
            if "setup_status" in config:
                self.setup_status = config["setup_status"]
            if "init_status" in config:
                self.init_status = config["init_status"]

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
        if self.ip and self.root_pw and self.initial_root_pw and self.user and self.user_pw:
            return True
        else:
            return False
    
    def has_setup_parameters(self):
        if self.hostname and self.user and self.user_pw and self.swap and self.bucket and self.coinmarketcap_api_key:
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

    @st.fragment
    def view_log_init(self):
        log = Path(f'{self.path}/vps_init.log')
        if log.exists():
            with open(log, 'r', encoding='utf-8') as f:
                ansi = f.read()
                ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
                result = ansi_escape.sub("", ansi)
                st.code(result, language="coffeescript")
                if st.button(":material/refresh:", key=f'refresh_view_log_{log}'):
                    st.rerun(scope="fragment")

    @st.fragment
    def view_log_setup(self):
        log = Path(f'{self.path}/vps_setup.log')
        if log.exists():
            with open(log, 'r', encoding='utf-8') as f:
                ansi = f.read()
                ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
                result = ansi_escape.sub("", ansi)
                st.code(result, language="coffeescript")
                if st.button(":material/refresh:", key=f'refresh_view_log_{log}'):
                    st.rerun(scope="fragment")

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

    def remove_init_log(self):
        log = Path(f'{self.path}/vps_init.log')
        if log.exists():
            log.unlink()
    
    def remove_setup_log(self):
        log = Path(f'{self.path}/vps_setup.log')
        if log.exists():
            log.unlink()

    def init_status_handler(self, status_data, runner_config):
        self.init_status = status_data["status"]

    def setup_status_handler(self, status_data, runner_config):
        self.setup_status = status_data["status"]

    def init_finished(self, runner_config=None):
        self.last_init = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save()
        shutil.rmtree(f'{self.path}/tmp', ignore_errors=True)

    def setup_finished(self, runner_config=None):
        self.last_setup = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save()
        shutil.rmtree(f'{self.path}/tmp', ignore_errors=True)

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
                "setup_status": self.setup_status,
                "init_status": self.init_status
            }
            with open(file, "w", encoding='utf-8') as f:
                json.dump(config, f, indent=4)
    
class VPSManager:
    def __init__(self):
        self.vpss = []
        self.path = Path(f'{PBGDIR}/data/vpsmanager/hosts')
        self.find_vps()
    
    def find_vps(self):
        p = str(Path(f'{PBGDIR}/data/vpsmanager/hosts/*/*.json'))
        hosts = glob.glob(p, recursive=False)
        if hosts:
            for host in hosts:
                v = VPS()
                v.path = Path(host)
                v.load()
                self.vpss.append(v)

    def add_vps(self):
        return VPS()

    def init_vps(self, vps : VPS, debug = False):
        vps.setup_status = None
        vps.save()
        vps.remove_init_log()
        vps.init_log = ""
        if debug:
            tags = "debug,all"
        else:
            tags = None
        ansible_runner.run_async(
            playbook=str(PurePath(f'{PBGDIR}/vps-init.yml')),
            inventory=vps.hostname,
            extravars={
                'hostname': vps.hostname,
                'ip': vps.ip,
                'initial_root_pw': shlex.quote(vps.initial_root_pw),
                'root_pw': shlex.quote(vps.root_pw),
                'user': vps.user,
                'user_pw': shlex.quote(vps.user_pw),
                'debug': debug
            },
            # quiet=True,
            tags=tags,
            verbosity=1,
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
        else:
            tags = None
        ansible_runner.run_async(
            playbook=str(PurePath(f'{PBGDIR}/vps-setup.yml')),
            inventory=vps.hostname,
            extravars={
                'hostname': vps.hostname,
                'user': vps.user,
                'user_pw': shlex.quote(vps.user_pw),
                'swap_size': vps.swap,
                'bucket': vps.bucket,
                'coinmarketcap_api_key': vps.coinmarketcap_api_key,
                'debug': debug
            },
            quiet=True,
            tags=tags,
            verbosity=1,
            private_data_dir=vps.privat_data_dir,
            event_handler=vps.setup_event_handler,
            status_handler=vps.setup_status_handler,
            finished_callback=vps.setup_finished
        )


def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
