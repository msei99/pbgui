import asyncio
import re
import psutil
import subprocess
import sys
import os
from pathlib import Path, PurePath
from time import sleep
import platform
import traceback
from api_key_state import get_user_state
from pbgui_func import PBGDIR
from telegram import Bot
from PBRemote import PBRemote
from pbgui_purefunc import load_ini, save_ini
from logging_helpers import human_log as _log


def _atomic_write_text(path: Path, content: str):
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tmp.open("w", encoding="utf-8") as f:
            f.write(content)
        tmp.replace(path)
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise


def _kill_process(process: psutil.Process, context: str):
    try:
        process.kill()
        process.wait(timeout=3)
    except psutil.NoSuchProcess:
        pass
    except psutil.TimeoutExpired:
        _log("PBMon", f"Timed out waiting for process to stop ({context})", level="WARNING")
    except psutil.AccessDenied as e:
        _log("PBMon", f"Access denied while stopping process ({context}): {e}", level="ERROR")

class PBMon():
    def __init__(self):
        self.piddir = Path(f'{PBGDIR}/data/pid')
        self.piddir.mkdir(parents=True, exist_ok=True)
        self.pidfile = Path(f'{self.piddir}/pbmon.pid')
        self.my_pid = None
        self.offline_error = []
        self.system_error = []
        self.instance_error = []
        self.pbremote = PBRemote()
        self._telegram_token = ""
        self._telegram_chat_id = ""
        self._hl_expiry_last_warned: dict[str, str] = {}  # {username: "YYYY-MM-DD"}
        
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
                    _log("PBMon", "Can not start PBMon", level="ERROR")
                    break
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            _log("PBMon", "Stop: PBMon")
            try:
                _kill_process(psutil.Process(self.my_pid), "PBMon")
            except psutil.NoSuchProcess:
                pass

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
                pid = f.read().strip()
                try:
                    self.my_pid = int(pid) if pid.isnumeric() else None
                except ValueError:
                    self.my_pid = None

    def save_pid(self):
        self.my_pid = os.getpid()
        _atomic_write_text(self.pidfile, str(self.my_pid))
    
    async def send_telegram_message(self, message):
        bot = Bot(token=self.telegram_token)
        async with bot:
            await bot.send_message(chat_id=self.telegram_chat_id, text=message, parse_mode='Markdown')

    async def check_hl_expiry(self):
        """Check HL API key expiry and send Telegram warnings."""
        from datetime import datetime, timezone
        warning_days = 7
        warning_days_str = load_ini("hl_expiry", "telegram_warning_days")
        if warning_days_str:
            try:
                warning_days = int(warning_days_str)
            except ValueError:
                warning_days = 7
        if warning_days < 1:
            return

        today_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

        try:
            from User import Users
            users = Users()
        except Exception as e:
            _log("PBMon", f"HL expiry check: failed to load users: {e}", level="WARNING")
            return

        warnings = []
        for user in users:
            if user.exchange != "hyperliquid":
                continue
            vu = get_user_state(user.name).get("hl_valid_until")
            if vu is None:
                continue
            try:
                vu = int(vu)
                expiry_dt = datetime.fromtimestamp(vu / 1000, tz=timezone.utc)
                days = (expiry_dt - datetime.now(tz=timezone.utc)).days
            except (ValueError, TypeError, OSError):
                continue

            if days <= warning_days:
                # Only warn once per day per user
                last = self._hl_expiry_last_warned.get(user.name)
                if last == today_str:
                    continue
                self._hl_expiry_last_warned[user.name] = today_str
                if days < 0:
                    warnings.append(f"⚠️ *{user.name}*: HL key EXPIRED ({-days}d ago)")
                elif days == 0:
                    warnings.append(f"⚠️ *{user.name}*: HL key expires TODAY")
                else:
                    warnings.append(f"⚠️ *{user.name}*: HL key expires in {days}d ({expiry_dt.strftime('%Y-%m-%d')})")

        if warnings:
            msg = "🔑 *HL API Key Expiry Warning*\n" + "\n".join(warnings)
            _log("PBMon", f"HL expiry warning: {len(warnings)} key(s)")
            await self.send_telegram_message(msg)

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
            offline_servers = {e["server"] for e in errors if e["name"] == "offline"}
            system_servers = {e["server"] for e in errors if e["name"] == "system"}
            instance_names = {e["name"] for e in errors if e["name"] not in ("offline", "system")}
            self.offline_error = [s for s in self.offline_error if s in offline_servers]
            self.system_error = [s for s in self.system_error if s in system_servers]
            self.instance_error = [n for n in self.instance_error if n in instance_names]
            msg = re.sub(r':blue\[(.*?)\]', r'*\1*', msg)
            msg = re.sub(r':red\[(.*?)\]', r'*\1*', msg)
            msg = re.sub(r':green\[(.*?)\]', r'\1', msg)
            msg = re.sub(r':orange\[(.*?)\]', r'\1', msg)
            if msg:
                _log("PBMon", f"Send Message: {msg.strip()}")
                await self.send_telegram_message(msg)
   

def main():
    pbmon = PBMon()
    if pbmon.is_running():
        _log("PBMon", "PBMon already started", level="ERROR")
        sys.exit(1)
    _log("PBMon", "Start: PBMon")
    pbmon.save_pid()
    while True:
        try:
            if pbmon.telegram_token and pbmon.telegram_chat_id:
                asyncio.run(pbmon.has_errors())
                asyncio.run(pbmon.check_hl_expiry())
            sleep(60)
        except Exception as e:
            _log("PBMon", f"Something went wrong, but continue: {e}", level="ERROR")
            _log("PBMon", "PBMon main loop traceback", level="DEBUG", meta={"traceback": traceback.format_exc()})

if __name__ == '__main__':
    main()