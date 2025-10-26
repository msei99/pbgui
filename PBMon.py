import asyncio
import re
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
from telegram import Bot
from PBRemote import PBRemote
import re
from pbgui_purefunc import load_ini, save_ini

class PBMon():
    def __init__(self):
        self.piddir = Path(f'{PBGDIR}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbmon.pid')
        self.my_pid = None
        self.offline_error = []
        self.system_error = []
        self.instance_error = []
        self.pbremote = PBRemote()
        self._telegram_token = ""
        self._telegram_chat_id = ""
        
    @property
    def telegram_token(self):
        if not self._telegram_token:
            self._telegram_token = load_ini("main", "telegram_token")
        return self._telegram_token
    @telegram_token.setter
    def telegram_token(self, new_telegram_token):
        if self._telegram_token != new_telegram_token:
            self._telegram_token = new_telegram_token
            save_ini("main", "telegram_token", new_telegram_token)

    @property
    def telegram_chat_id(self):
        if not self._telegram_chat_id:
            self._telegram_chat_id = load_ini("main", "telegram_chat_id")
        return self._telegram_chat_id
    @telegram_chat_id.setter
    def telegram_chat_id(self, new_telegram_chat_id):
        if self._telegram_chat_id != new_telegram_chat_id:
            self._telegram_chat_id = new_telegram_chat_id
            save_ini("main", "telegram_chat_id", new_telegram_chat_id)
    
    def run(self):
        if not self.is_running():
            cmd = [sys.executable, '-u', PurePath(f'{PBGDIR}/PBMon.py')]
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=PBGDIR, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=PBGDIR, text=True, start_new_session=True)
            count = 0
            while True:
                if count > 5:
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: Can not start PBMon')
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: PBMon')
            psutil.Process(self.my_pid).kill()

    def restart(self):
        if self.is_running():
            self.stop()
            self.run()

    def is_running(self):
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(sub.lower().endswith("pbmon.py") for sub in psutil.Process(self.my_pid).cmdline()):
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
    
    async def send_telegram_message(self, message):
        bot = Bot(token=self.telegram_token)
        async with bot:
            await bot.send_message(chat_id=self.telegram_chat_id, text=message, parse_mode='Markdown')

    async def has_errors(self):
        self.pbremote.update_remote_servers()
        errors = self.pbremote.has_error()
        if errors:
            msg = ""
            for error in errors:
                if error["name"] == "offline":
                    if error["server"] not in self.offline_error:
                        self.offline_error.append(error["server"])
                        msg = msg + f'Server: *{error["server"]}* is offline\n'
                elif error["name"] == "system":
                    if error["server"] not in self.system_error:
                        self.system_error.append(error["server"])
                        msg = msg + f'Server: {error["server"]} Instance: {error["name"]} Mem: {error["mem"]} CPU: {error["cpu"]} Swap: {error["swap"]} Disk: {error["disk"]}\n'
                else:
                    if error["name"] not in self.instance_error:
                        self.instance_error.append(error["name"])
                        msg = msg + f'Server: {error["server"]} Instance: {error["name"]} Mem: {error["mem"]} Swap: {error["swap"]} CPU: {error["cpu"]} Error: {error["error"]} Traceback: {error["traceback"]}\n'
            # remove errors that are no longer present
            self.offline_error = [error for error in self.offline_error if error in [error["server"] for error in errors if error["name"] == "offline"]]
            self.system_error = [error for error in self.system_error if error in [error["server"] for error in errors if error["name"] == "system"]]
            self.instance_error = [error for error in self.instance_error if error in [error["name"] for error in errors if error["name"] not in ["offline", "system"]]]
            msg = re.sub(r':blue\[(.*?)\]', r'*\1*', msg)
            msg = re.sub(r':red\[(.*?)\]', r'*\1*', msg)
            msg = re.sub(r':green\[(.*?)\]', r'\1', msg)
            msg = re.sub(r':orange\[(.*?)\]', r'\1', msg)
            if msg:
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Send Message:\n{msg}')
                await self.send_telegram_message(msg)
   

def main():
    dest = Path(f'{PBGDIR}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    logfile = Path(f'{str(dest)}/PBMon.log')
    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: PBMon')
    pbmon = PBMon()
    if pbmon.is_running():
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: PBMon already started')
        exit(1)
    pbmon.save_pid()
    while True:
        try:
            if logfile.exists():
                if logfile.stat().st_size >= 10485760:
                    logfile.replace(f'{str(logfile)}.old')
                    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
                    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
            if pbmon.telegram_token and pbmon.telegram_chat_id:
                asyncio.run(pbmon.has_errors())
            sleep(60)
        except Exception as e:
            print(f'Something went wrong, but continue {e}')
            traceback.print_exc()

if __name__ == '__main__':
    main()