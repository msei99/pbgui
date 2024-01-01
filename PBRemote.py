import psutil
import subprocess
import configparser
import sys
from pathlib import Path, PurePath
from time import sleep
import glob
import json
from io import TextIOWrapper
from datetime import datetime
import platform
from PBRun import PBRun, RunInstance
import uuid
import shutil
import hashlib

class RemoteServer():
    def __init__(self, path: str):
        self._name = None
        self._ts = None
        self._rtd = None
        self._run = None
        self._edit = False
        self._instances = False
        self._path = path
        self._unique = []
        self._api_md5 = None
        self._pbdir = None
    
    @property
    def name(self): return self._name
    @property
    def ts(self): return self._ts
    @property
    def rtd(self): return self._rtd
    @property
    def run(self): return self._run
    @property
    def edit(self): return self._edit
    @property
    def instances(self): return self._instances
    @property
    def path(self): return self._path
    @property
    def api_md5(self): return self._api_md5
    @property
    def pbdir(self): return self._pbdir

    @name.setter
    def name(self, new_name):
        if self._name != new_name:
            self._name = new_name
    @ts.setter
    def ts(self, new_ts):
        if self._ts != new_ts:
            self._ts = new_ts
    @edit.setter
    def edit(self, new_edit):
        if self._edit != new_edit:
            self._edit = new_edit
    @instances.setter
    def instances(self, new_instances):
        if self._instances != new_instances:
            self._instances = new_instances
    @path.setter
    def path(self, new_path):
        if self._path != new_path:
            self._path = new_path
    @pbdir.setter
    def pbdir(self, new_pbdir):
        if self._pbdir != new_pbdir:
            self._pbdir = new_pbdir

    def is_running(self, user : str, symbol : str):
        self.load()
        if self.run:
            for running in self.run:
                if running["user"] == user and running["symbol"] == symbol:
                    return True
        if self.has_instance(user,symbol):
            return False
        else:
            return None

    def has_instance(self, user : str, symbol : str):
        p = str(Path(f'{self._path}/../instances_{self.name}/*'))
        instances = glob.glob(p)
        for instance in instances:
            file = Path(f'{instance}/instance.cfg')
            if file.exists():
                with open(file, "r", encoding='utf-8') as f:
                    config = json.load(f)
                    if config["_user"] == user and config["_symbol"] == symbol:
                        return True
        return False

    def is_api_md5_same(self, api_md5 : str):
        if self.api_md5 == api_md5:
            return True
        return False

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
                if "api_md5" in cfg:
                    self._api_md5 = cfg["api_md5"]
                if "run" in cfg:
                    self._run = cfg["run"]

    def send_to(self, command : str, user : str = None, symbol : str = None, market_type : str = None):
        unique = str(uuid.uuid4())
        timestamp = round(datetime.now().timestamp())
        if user:
            instance = f'{user}_{symbol}_{market_type}'
        else:
            instance = "all"
        cfile = str(Path(f'{self._path}/../../cmd/send_{self.name}_{unique}.cmd'))
        cfg = ({
            "timestamp": timestamp,
            "unique": unique,
            "to": self.name,
            "command": command,
            "instance": instance
            })
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)
#        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} sync_to: {self.name} {command} {instance}')

    def ack_to(self, command : str, instance : str, unique : str):
        timestamp = round(datetime.now().timestamp())
        cfile = str(Path(f'{self._path}/../../cmd/{self.name}_{unique}.ack'))
        cfg = ({
            "timestamp": timestamp,
            "unique": unique,
            "to": self.name,
            "command": command,
            "instance": instance
            })
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} ack_to: {self.name} {command} {instance} {unique}')

    def ack_from(self, pbname : str):
        p = str(Path(f'{self._path}/{pbname}_*.ack'))
        ack_remote = glob.glob(p)
        if ack_remote:
            for ack in ack_remote:
                remote = Path(ack)
                with open(remote, "r", encoding='utf-8') as f:
                    cfg = json.load(f)
                    if "to" in cfg and "unique" in cfg:
                        to = cfg["to"]
                        if to == pbname:
                            unique = cfg["unique"]
                            instance = cfg["instance"]
                            command = cfg["command"]
                            if command == "sync_api":
                                cfile = Path(f'{self._path}/../../cmd/api-keys.json')
                                if cfile.exists():
                                    cfile.unlink(missing_ok=True)
                            cfile = Path(f'{self._path}/../../cmd/sync_{self.name}_{unique}.cmd')
                            if cfile.exists():
                                cfile.unlink(missing_ok=True)
                                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} ack_from: {self.name} {command} {instance} {unique}')
                                return True

    def sync_from(self, pbname : str):
        p = str(Path(f'{self._path}/sync_{pbname}_*.cmd'))
        sync_remote = glob.glob(p)
        if sync_remote:
            for sync in sync_remote:
                remote = Path(sync)
                with open(remote, "r", encoding='utf-8') as f:
                    cfg = json.load(f)
                    if "to" in cfg and "instance" in cfg and "unique" in cfg:
                        to = cfg["to"]
                        if to == pbname:
                            command = cfg["command"]
                            instance = cfg["instance"]
                            unique = cfg["unique"]
                            if unique not in self._unique:
                                if command == "sync_api":
                                    api_keys = PurePath(f'{self._pbdir}/api-keys.json')
                                    # Backup api-keys
                                    date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                                    api_backup = Path(f'{self._path}/../../api-keys')
                                    if not api_backup.exists():
                                        api_backup.mkdir(parents=True)
                                    backup_dest = Path(f'{api_backup}/api-keys_{date}.json')
                                    shutil.copy(api_keys, backup_dest)
                                    # Copy new api-keys
                                    src = PurePath(f'{self._path}/api-keys.json')
                                    shutil.copy(src, api_keys)
                                elif command == "sync":
                                    self.sync(pbname)
                                    src = PurePath(f'{self._path}/../instances_{self.name}/{instance}')
                                    dest = PurePath(f'{self._path}/../../instances/{instance}')
                                    shutil.copytree(src, dest, dirs_exist_ok=True)
                                    PBRun().disable_instance(instance)
                                elif command == "remove":
                                    dest = PurePath(f'{self._path}/../../instances/{instance}')
                                    shutil.rmtree(dest, ignore_errors=True)
                                elif command == "start":
                                    PBRun().start_instance(instance)
                                elif command == "stop":
                                    PBRun().stop_instance(instance)
                                else:
                                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} sync_from: unknown command {self.name} {command} {instance} {unique}')    
                                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} sync_from: {self.name} {command} {instance} {unique}')
                                self.ack_to(command, instance, unique)
                                self._unique.append(unique)
                                return True
        else:
            p = str(Path(f'{self.path}/../../cmd/*.ack'))
            sync_ack = glob.glob(p)
            if sync_ack:
                for file in sync_ack:
                    afile = Path(file)
                    with open(afile, "r", encoding='utf-8') as f:
                        cfg = json.load(f)
                        to = cfg["to"]
                        unique = cfg["unique"]
                        instance = cfg["instance"]
                        command = cfg["command"]
                        if to == self.name:
                            if unique in self._unique:
                                self._unique.remove(unique)
                            afile.unlink(missing_ok=True)
                            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} remove_ack: {self.name} {command} {instance} {unique}')

    def sync(self, pbname: str):
        pbgdir = Path.cwd()
        spath = 'instances'
        cmd = ['rclone', 'sync', '-v', '--exclude', f'{{{spath}_{pbname}/*,cmd_**}}', f'pbgui:pbgui', PurePath(f'{pbgdir}/data/remote')]
        logfile = Path(f'{pbgdir}/data/logs/sync.log')
        log = open(logfile,"ab")
        subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True)

class PBRemote():
    def __init__(self):
        self.remote_servers = []
        self.local_run = PBRun()
        self.index = 0
        self.api_md5 = None
        pbgdir = Path.cwd()
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_option("main", "pbname"):
            self.name = pb_config.get("main", "pbname")
        else:
            self.name = platform.node()
        self.pbdir = pb_config.get("main", "pbdir")
        self.instances_path = f'{pbgdir}/data/instances'
        self.cmd_path = f'{pbgdir}/data/cmd'
        self.remote_path = f'{pbgdir}/data/remote'
        if not Path(self.cmd_path).exists():
            Path(self.cmd_path).mkdir(parents=True)            
        self.load_remote()
        self.load_local()


    def __iter__(self):
        return iter(self.remote_servers)

    def __next__(self):
        if self.index > len(self.remote_servers):
            raise StopIteration
        self.index += 1
        return next(self)

    def list(self):
        return list(map(lambda c: c.name, self.remote_servers))

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
            cmd = ['rclone', 'sync', '-v', '--include', f'{{alive_*.cmd,sync_*.cmd,*.ack,api-keys.json}}', PurePath(f'{pbgdir}/data/{spath}'), f'pbgui:pbgui/{spath}_{self.name}']
        elif direction == 'up' and spath == 'instances':
            cmd = ['rclone', 'sync', '-v', '--include', f'{{instance.cfg,config.json}}', PurePath(f'{pbgdir}/data/{spath}'), f'pbgui:pbgui/{spath}_{self.name}']
        elif direction == 'down' and spath == 'cmd':
            cmd = ['rclone', 'sync', '-v', '--exclude', f'{{{spath}_{self.name}/*,instances_**}}', f'pbgui:pbgui', PurePath(f'{pbgdir}/data/remote')]
        elif direction == 'down' and spath == 'instances':
            cmd = ['rclone', 'sync', '-v', '--exclude', f'{{{spath}_{self.name}/*,cmd_**}}', f'pbgui:pbgui', PurePath(f'{pbgdir}/data/remote')]
        logfile = Path(f'{pbgdir}/data/logs/sync.log')
        if logfile.exists():
            if logfile.stat().st_size >= 10485760:
                logfile.replace(f'{pbgdir}/data/logs/sync.log.old')
                logfile = Path(f'{pbgdir}/data/logs/sync.log')
        log = open(logfile,"ab")
        subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True)
#        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: {cmd}')

    def sync_to(self):
        p = str(Path(f'{self.cmd_path}/send_*.cmd'))
        sync_cmd = glob.glob(p)
        if sync_cmd:
            for file in sync_cmd:
                cfile = Path(file)
                with open(cfile, "r", encoding='utf-8') as f:
                    cfg = json.load(f)
                    to = cfg["to"]
                    unique = cfg["unique"]
                    instance = cfg["instance"]
                    command = cfg["command"]
                    if command == "sync_api":
                        src = PurePath(f'{self.pbdir}/api-keys.json')
                        dest = PurePath(f'{self.cmd_path}/api-keys.json')
                        shutil.copy(src, dest)
                    if command == "copy":
                        src = PurePath(f'{self.remote_path}/instances_{to}/{instance}')
                        dest = PurePath(f'{self.instances_path}/{instance}')
                        shutil.copytree(src, dest, dirs_exist_ok=True)
                        PBRun().disable_instance(instance)
                        cfile.unlink(missing_ok=True)
                        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} sync_from: {to} {command} {instance} {unique}')
                    if command == "sync":
                        self.sync('up', 'instances')
                    if command in ['start','stop','sync','sync_api','remove']:
                        cfile.rename(f'{self.cmd_path}/sync_{to}_{unique}.cmd')
                        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} sync_to: {to} {command} {instance} {unique}')


    def alive(self):
        timestamp = round(datetime.now().timestamp())
        cfile = Path(f'{self.cmd_path}/alive_{timestamp}.cmd')
        run = []
        for instance in self.local_run:
            inst = ({
                "user": instance.user,
                "symbol": instance.symbol
            })
            run.append(inst)
        cfg = ({
            "timestamp": timestamp,
            "name": self.name,
            "api_md5": self.api_md5,
            "run": run
            })
        with open(cfile, "w", encoding='utf-8') as f:
            json.dump(cfg, f)
        self.sync('up', 'cmd')
        p = str(Path(f'{self.cmd_path}/alive_*.cmd'))
        found_local = glob.glob(p)
        found_local.sort()
        while len(found_local) > 9:
            local = Path(found_local.pop(0))
            local.unlink(missing_ok=True)

    def calculate_api_md5(self):
        file = Path(f'{self.pbdir}/api-keys.json')
        with open(file, 'rb') as file_obj:
            file_contents = file_obj.read()
        return hashlib.md5(file_contents).hexdigest()

    def load_remote(self):
        pbgdir = Path.cwd()
        self.remote_servers = []
        p = str(Path(f'{pbgdir}/data/remote/cmd_*'))
        found_remote = glob.glob(p)
        for remote in found_remote:
            rserver = RemoteServer(remote)
            rserver.pbdir = self.pbdir
            rserver.load()
            self.add(rserver)

    def load_local(self):
        self.local_run.load_all()
        self.api_md5 = self.calculate_api_md5()

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
    remote.sync('up', 'instances')
    remote.sync('down', 'instances')
    while True:
        try:
            remote.alive()
            remote.sync_to()
            remote.sync('down', 'cmd')
            for server in remote.remote_servers:
                server.load()
                if server.sync_from(remote.name):
                    remote.sync("up", 'instances')
                    remote.load_local()
                if server.ack_from(remote.name):
                    remote.sync("down", 'instances')
#            sleep(2)
        except Exception as e:
            print(f'Something went wrong, but continue {e}')

if __name__ == '__main__':
    main()