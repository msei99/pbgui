import psutil
import subprocess
import sys
import os
import configparser
from pathlib import Path, PurePath
from time import sleep
from io import TextIOWrapper
from datetime import datetime
from Instance import Instances
import platform
import traceback
import jinja2

class PBShare(Instances):
    def __init__(self):
        super().__init__()
        pbgdir = Path.cwd()
        self.piddir = Path(f'{pbgdir}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.griddir = Path(f'{pbgdir}/data/grid')
        if not self.griddir.exists():
            self.griddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbshare.pid')
        self.my_pid = None
        self.bucket = None
        self.upload_images = False
        self.interval = 1800
        self.load_config()
        self.buckets = self.fetch_buckets()
        self.rclone_installed = self.is_rclone_installed()

    def load_config(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_section("pbshare"):
            if pb_config.has_option("pbshare", "bucket"):
                self.bucket = pb_config.get("pbshare", "bucket")
            else:
                self.bucket = None
            if pb_config.has_option("pbshare", "upload_images"):
                self.upload_images = pb_config.getboolean("pbshare", "upload_images")
            else:
                self.upload_images = False
            if pb_config.has_option("pbshare", "interval"):
                self.interval = pb_config.getint("pbshare", "interval")
            else:
                self.interval = 1800

    def save_config(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("pbshare"):
            pb_config.add_section("pbshare")
        pb_config.set("pbshare", "bucket", self.bucket)
        pb_config.set("pbshare", "upload_images", str(self.upload_images))
        pb_config.set("pbshare", "interval", str(self.interval))
        with open('pbgui.ini', 'w') as configfile:
            pb_config.write(configfile)

    def is_rclone_installed(self):
        cmd = ['rclone', 'version']
        try:
            subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return True
        except FileNotFoundError:
            return False
    
    def fetch_buckets(self):
        if self.is_rclone_installed():
            cmd = ['rclone', 'listremotes']
            try:
                result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if result.returncode == 0:
                    return result.stdout.splitlines()
            except FileNotFoundError:
                pass
        return []

    def run(self):
        if not self.is_running():
            pbgdir = Path.cwd()
            cmd = [sys.executable, '-u', PurePath(f'{pbgdir}/PBShare.py')]
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, start_new_session=True)
            count = 0
            while True:
                if count > 5:
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: Can not start PBShare')
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: PBShare')
            psutil.Process(self.my_pid).kill()

    def restart(self):
        if self.is_running():
            self.stop()
            self.run()

    def is_running(self):
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(sub.lower().endswith("pbshare.py") for sub in psutil.Process(self.my_pid).cmdline()):
                return True
        except psutil.NoSuchProcess:
            pass
        return False

    def load_pid(self):
        if self.pidfile.exists():
            with open(self.pidfile) as f:
                pid = f.read()
                self.my_pid = int(pid) if pid.isnumeric() else None

    def save_pid(self):
        self.my_pid = os.getpid()
        with open(self.pidfile, 'w') as f:
            f.write(str(self.my_pid))

    def generate_grid_picture(self, remove: bool = False):
        grids = {}
        for instance in self.instances:
            if instance.pbshare_grid:
                if instance.user not in grids:
                    grids[instance.user] = []
                grids[instance.user].append(instance.symbol)
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Generate grid picture for {instance.user}_{instance.symbol}')
                instance.save_ohlcv()
                if self.upload_images:
                    if remove:
                        self.remove_old_versions(instance.user, f'{instance.instance_path}/grid_{instance.user}_{instance.symbol}.png')
                    self.sync_pbshare(instance.user, f'{instance.instance_path}/grid_{instance.user}_{instance.symbol}.png')
        self.generate_index(grids)

    def sync_pbshare(self, user: str, file: str):
        if self.bucket and self.rclone_installed:
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Upload new grid picture {file}')
            cmd = ['rclone', 'copy', '-v', file, f'{self.bucket}{user}'] 
            self.rclone(cmd)
    
    def remove_old_versions(self, user: str, file: str):
        if self.bucket and self.rclone_installed:
            filename = file.split('/')[-1]
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Remove old versions of grid picture {filename}')
            cmd = ['rclone', 'deletefile', '-v', f'{self.bucket}{user}/{filename}']
            self.rclone(cmd)

    def rclone(self, cmd: str):
        pbgdir = Path.cwd()
        logfile = Path(f'{pbgdir}/data/logs/pbshare.log')
        log = open(logfile,"ab")
        if platform.system() == "Windows":
            creationflags = subprocess.CREATE_NO_WINDOW
            subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True, creationflags=creationflags)
        else:
            subprocess.run(cmd, stdout=log, stderr=log, cwd=pbgdir, text=True)

    def generate_index(self, grids: dict):
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Generate index.html')
        with open('index.html.jinja') as f:
            template = jinja2.Template(f.read())
            index = template.render(grids=grids)
        with open(f'{self.griddir}/index.html', 'w') as f: 
            f.write(index)

def main():
    pbgdir = Path.cwd()
    dest = Path(f'{pbgdir}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    logfile = Path(f'{str(dest)}/PBShare.log')
    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: PBShare')
    share = PBShare()
    if share.is_running():
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: PBShare already started')
        exit(1)
    share.save_pid()
    timestamp = round(datetime.now().timestamp())
    while True:
        try:
            if logfile.exists():
                if logfile.stat().st_size >= 10485760:
                    logfile.replace(f'{str(logfile)}.old')
                    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
                    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
            if timestamp + 21600 < round(datetime.now().timestamp()):
                remove = True
                timestamp = round(datetime.now().timestamp())
            else:
                remove = False
            share.generate_grid_picture(remove)
            sleep(share.interval)
            share.reload_instances()
        except Exception as e:
            print(f'Something went wrong, but continue {e}')
            traceback.print_exc()


if __name__ == '__main__':
    main()