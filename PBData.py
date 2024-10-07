import psutil
import subprocess
import sys
import os
from pathlib import Path, PurePath
from time import sleep
from io import TextIOWrapper
from datetime import datetime
import platform
import traceback
from pbgui_func import PBGDIR
from Database import Database
from User import Users
import configparser

class PBData():
    def __init__(self):
        self.piddir = Path(f'{PBGDIR}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbdata.pid')
        self.my_pid = None
        self.db = Database()
        self.users = Users()
        self._fetch_users = self.load_fetch_users()

    # fetch_users
    @property
    def fetch_users(self):
        return self._fetch_users
    @fetch_users.setter
    def fetch_users(self, new_fetch_users):
        self._fetch_users = new_fetch_users
        self.save_fetch_users()

    def run(self):
        if not self.is_running():
            cmd = [sys.executable, '-u', PurePath(f'{PBGDIR}/PBData.py')]
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=PBGDIR, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=PBGDIR, text=True, start_new_session=True)
            count = 0
            while True:
                if count > 5:
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: Can not start PBData')
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: PBData')
            psutil.Process(self.my_pid).kill()

    def restart(self):
        if self.is_running():
            self.stop()
            self.run()

    def is_running(self):
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(sub.lower().endswith("pbdata.py") for sub in psutil.Process(self.my_pid).cmdline()):
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
    
    def load_fetch_users(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_option("pbdata", "fetch_users"):
            users = eval(pb_config.get("pbdata", "fetch_users"))
            for user in users.copy():
                if user not in self.users.list():
                    users.remove(user)
            return users
        return []
    
    def save_fetch_users(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("pbdata"):
            pb_config.add_section("pbdata")
        pb_config.set("pbdata", "fetch_users", f'{self.fetch_users}')
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)

    def update_db(self):
        for user in self.users:
            if user.name in self.fetch_users:
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Fetch history for {user.name}')
                self.db.update_history(user)
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Fetch positions for {user.name}')
                self.db.update_positions(user)
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Fetch orders for {user.name}')
                self.db.update_orders(user)
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Fetch prices for {user.name}')
                self.db.update_prices(user)
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Fetch balance for {user.name}')
                self.db.update_balances(user)

def main():
    dest = Path(f'{PBGDIR}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    logfile = Path(f'{str(dest)}/PBData.log')
    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: PBData')
    pbdata = PBData()
    if pbdata.is_running():
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: PBData already started')
        exit(1)
    pbdata.save_pid()
    while True:
        try:
            if logfile.exists():
                if logfile.stat().st_size >= 10485760:
                    logfile.replace(f'{str(logfile)}.old')
                    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
                    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
            pbdata.update_db()
            sleep(1)
        except Exception as e:
            print(f'Something went wrong, but continue {e}')
            traceback.print_exc()

if __name__ == '__main__':
    main()