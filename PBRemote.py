import psutil
import subprocess
import configparser
import shlex
import sys
from pathlib import Path, PurePath
from time import sleep
import glob
import json
from io import TextIOWrapper
from datetime import datetime
import platform

class RemoteServer():
    def __init__(self, path: str):
        self._name = None
        self._ts = None
        self._rtd = None
        self._path = path
    
    @property
    def name(self): return self._name
    @property
    def ts(self): return self._ts
    @property
    def rtd(self): return self._rtd
    @property
    def path(self): return self._path

    @name.setter
    def name(self, new_name):
        if self._name != new_name:
            self._name = new_name
    @ts.setter
    def ts(self, new_ts):
        if self._ts != new_ts:
            self._ts = new_ts
    @path.setter
    def path(self, new_path):
        if self._path != new_path:
            self._path = new_path

    def is_online(self):
        self.load()
        timestamp = round(datetime.now().timestamp())
        self._rtd = timestamp - self.ts
        if self._rtd < 60:
            return True
        return False

    def load(self):
        p = str(Path(f'{self._path}/alive_*.cmd'))
        alive_remote = glob.glob(p)
        alive_remote.sort()
        if alive_remote:
            remote = Path(alive_remote.pop())
            with open(remote, "r", encoding='utf-8') as f:
                cfg = json.load(f)
                if "name" in cfg and "timestamp" in cfg:
                    self._name = cfg["name"]
                    self._ts = cfg["timestamp"]

class PBRemote():
    def __init__(self):
        self.remote_servers = []
        self.index = 0
        pbgdir = Path.cwd()
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_option("main", "pbname"):
            self.name = pb_config.get("main", "pbname")
        else:
            self.name = platform.node()
        self.instances_path = f'{pbgdir}/data/instances'
        self.cmd_path = f'{pbgdir}/data/cmd'
        if not Path(self.cmd_path).exists():
            Path(self.cmd_path).mkdir(parents=True)            

    def __iter__(self):
        return iter(self.remote_servers)

    def __next__(self):
        if self.index > len(self.remote_servers):
            raise StopIteration
        self.index += 1
        return next(self)

    def add(self, remote_servers: RemoteServer):
        if remote_servers:
            self.remote_servers.append(remote_servers)

    def remove(self, remote_servers: RemoteServer):
        if remote_servers:
            self.remote_servers.remove(remote_servers)

    def is_sync_running(self):
        if self.sync_pid():
            return True
        return False

    def sync_pid(self):
        for process in psutil.process_iter():
            try:
                cmdline = process.cmdline()
            except psutil.AccessDenied:
                continue
            if any("rclone" in sub for sub in cmdline) and any("pbgui:pbgui" in sub for sub in cmdline):
                return process

    def sync(self, direction: str, spath: str):
        pbgdir = Path.cwd()
        if direction == 'up' and spath == 'cmd':
            cmd = ['rclone', 'sync', '-v', PurePath(f'{pbgdir}/data/{spath}'), f'pbgui:pbgui/{spath}_{self.name}']
        if direction == 'up' and spath == 'instances':
            cmd = ['rclone', 'sync', '-v', '--include', f'{{instance.cfg,config.json}}', PurePath(f'{pbgdir}/data/{spath}'), f'pbgui:pbgui/{spath}_{self.name}']
        elif direction == 'down' and spath == 'cmd':
            cmd = ['rclone', 'sync', '-v', '--exclude', f'{{{spath}_{self.name}/*,instances_**}}', f'pbgui:pbgui', PurePath(f'{pbgdir}/data/remote')]
        logfile = Path(f'{pbgdir}/data/logs/sync.log')
        log = open(logfile,"ab")
        subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True)
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: {cmd}')

    def alive(self):
        timestamp = round(datetime.now().timestamp())
        cfile = Path(f'{self.cmd_path}/alive_{timestamp}.cmd')
        cfg = ({
            "timestamp": timestamp,
            "name": self.name})
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)
        self.sync('up', 'cmd')
        p = str(Path(f'{self.cmd_path}/alive_*.cmd'))
        found_local = glob.glob(p)
        found_local.sort()
        while len(found_local) > 9:
            local = Path(found_local.pop(0))
            local.unlink(missing_ok=True)

    # def has_remote(self):
    #     self.sync('down', 'cmd')
    #     pbgdir = Path.cwd()
    #     p = str(Path(f'{pbgdir}/data/remote/cmd_*/alive_*.cmd'))
    #     found_remote = glob.glob(p)
    #     found_remote.sort()
    #     if found_remote:
    #         remote = Path(found_remote.pop())
    #         with open(remote, "r", encoding='utf-8') as f:
    #             cfg = json.load(f)
    #             print(cfg)

    def load_remote(self):
        pbgdir = Path.cwd()
        self.remote_servers = []
        p = str(Path(f'{pbgdir}/data/remote/cmd_*'))
        found_remote = glob.glob(p)
        for remote in found_remote:
            rserver = RemoteServer(remote)
            rserver.load()
            self.add(rserver)

    def run(self):
        if not self.is_running():
            pbgdir = Path.cwd()
            cmd = [sys.executable, '-u', PurePath(f'{pbgdir}/PBRemote.py')]
            subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, start_new_session=True)

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: PBRemote')
            self.pid().kill()

    def is_running(self):
        if self.pid():
            return True
        return False

    def pid(self):
        for process in psutil.process_iter():
            try:
                cmdline = process.cmdline()
            except psutil.AccessDenied:
                continue
            if any("PBRemote.py" in sub for sub in cmdline):
                return process


def main():
    # Not supported on windows
    if platform.system() == "Windows":
        print("PBRemote Module is not supported on Windows")
        exit()
    pbgdir = Path.cwd()
    dest = Path(f'{pbgdir}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    sys.stdout = TextIOWrapper(open(Path(f'{dest}/PBRemote.log'),"ab",0), write_through=True)
    sys.stderr = TextIOWrapper(open(Path(f'{dest}/PBRemote.log'),"ab",0), write_through=True)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: PBRemote')
    remote = PBRemote()
    remote.load_remote()
    remote.sync('up', 'instances')
    while True:
        try:
            remote.alive()
            remote.sync('down', 'cmd')
            for server in remote.remote_servers:
                server.load()
            sleep(5)
        except Exception:
            print("Something went wrong, but continue")

if __name__ == '__main__':
    main()