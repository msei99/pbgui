"""
PBRun manages v7 passivbot instances.

It checks for status and activate files, starts and stops v7 bots accordingly.
"""
import psutil
import subprocess
import configparser
import shlex
import sys
from pathlib import Path, PurePath
from time import sleep, mktime
import glob
import json
import hjson
from datetime import datetime, date, timedelta
import platform
from shutil import copy, copytree, rmtree, ignore_patterns
import os
import traceback
import uuid
import copy as copy_module
from Status import InstanceStatus, InstancesStatus
from PBCoinData import CoinData
from logging_helpers import human_log as _log
import re


_PB7_FILL_SUMMARY_RE = re.compile(
    r"\[fill\]\s+(?P<count>\d+)\s+fills,\s+pnl=(?P<pnl>[+-]?(?:\d+\.?\d*|\d*\.\d+))\s+\w+"
)
_PB7_FILL_PNL_RE = re.compile(r"\bpnl=(?P<pnl>[+-]?(?:\d+\.?\d*|\d*\.\d+))\b")


def _arg_matches_path(arg: str, expected_path: Path) -> bool:
    if not arg:
        return False
    expected = str(expected_path)
    expected_alt = expected.replace("/", "\\")
    return str(arg).endswith(expected) or str(arg).endswith(expected_alt)


def _atomic_write_json(path: Path, payload, indent: int = None):
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tmp_path.open("w", encoding="utf-8") as f:
            if indent is None:
                json.dump(payload, f)
            else:
                json.dump(payload, f, indent=indent)
        tmp_path.replace(path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise


def _atomic_write_text(path: Path, content: str):
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tmp_path.open("w", encoding="utf-8") as f:
            f.write(content)
        tmp_path.replace(path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise


def _atomic_write_config(path: Path, parser: configparser.ConfigParser):
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tmp_path.open("w", encoding="utf-8") as f:
            parser.write(f)
        tmp_path.replace(path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise


def _attach_process_stats(process: psutil.Process, monitor: "Monitor"):
    monitor.start_time = process.create_time()
    try:
        monitor.memory = process.memory_full_info()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        monitor.memory = None
    try:
        monitor.cpu = process.cpu_percent()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        monitor.cpu = None


def _memory_usage_bytes(memory_info) -> int:
    if memory_info is None:
        return 0
    rss = getattr(memory_info, "rss", None)
    uss = getattr(memory_info, "uss", None)
    if isinstance(rss, (int, float)) and isinstance(uss, (int, float)):
        return int(rss + uss)
    if isinstance(rss, (int, float)):
        return int(rss)
    try:
        return int(memory_info[0]) + int(memory_info[9])
    except Exception:
        return 0


def _kill_process(process: psutil.Process, context: str):
    try:
        process.kill()
        process.wait(timeout=3)
    except psutil.NoSuchProcess:
        pass
    except psutil.TimeoutExpired:
        _log("PBRun", f"Timed out waiting for process to stop ({context})", level="WARNING")
    except psutil.AccessDenied as e:
        _log("PBRun", f"Access denied while stopping process ({context}): {e}", level="ERROR")


def _run_subprocess(
    command,
    *,
    timeout: int = 20,
    env: dict | None = None,
    capture_stdout: bool = True,
    suppress_stderr: bool = True,
):
    kwargs = {
        "text": True,
        "timeout": timeout,
    }
    kwargs["stdout"] = subprocess.PIPE if capture_stdout else subprocess.DEVNULL
    kwargs["stderr"] = subprocess.DEVNULL if suppress_stderr else subprocess.PIPE
    if env is not None:
        kwargs["env"] = env
    try:
        return subprocess.run(command, **kwargs)
    except subprocess.TimeoutExpired:
        _log("PBRun", f"Command timeout after {timeout}s: {' '.join(map(str, command))}", level="WARNING")
        return None
    except FileNotFoundError as e:
        _log("PBRun", f"Command not found: {' '.join(map(str, command))} ({e})", level="WARNING")
        return None
    except Exception as e:
        _log("PBRun", f"Command failed: {' '.join(map(str, command))} ({e})", level="ERROR")
        return None


def _parse_git_log_output(raw_output: str, parse_context: str):
    commits = []
    latest_commit_timestamp = None
    for commit_block in raw_output.split('\x00'):
        commit_block = commit_block.strip()
        if not commit_block:
            continue
        lines = commit_block.split('\n', 1)
        if not lines:
            continue
        parts = lines[0].split('|', 5)
        if len(parts) == 6:
            full_message = parts[5]
            if len(lines) > 1:
                full_message = full_message + '\n' + lines[1]
            commit_data = {
                'short': parts[0],
                'full': parts[1],
                'author': parts[2],
                'date': parts[3],
                'timestamp': int(parts[4]),
                'message': full_message.strip(),
            }
            commits.append(commit_data)
            if latest_commit_timestamp is None:
                latest_commit_timestamp = commit_data['timestamp']
        else:
            _log(
                "PBRun",
                f"Failed to parse commit block for {parse_context}: {len(parts)} parts, first 100 chars: {commit_block[:100]}",
                level="WARNING",
            )
    return commits, latest_commit_timestamp


def _ensure_dynamic_ignore_ready(dynamic_ignore: "DynamicIgnore") -> bool:
    if dynamic_ignore is None:
        return True
    list_files_exist = getattr(dynamic_ignore, "list_files_exist", None)
    if callable(list_files_exist) and list_files_exist():
        return True
    lists_ready = getattr(dynamic_ignore, "lists_ready", None)
    if callable(lists_ready) and lists_ready():
        return True
    watch = getattr(dynamic_ignore, "watch", None)
    if callable(watch):
        watch()
    if callable(lists_ready):
        return lists_ready()
    return False

class Monitor():
    def __init__(self):
        self.path = None
        self.user = None
        self.version = None
        self.pb_version = None
        self.log_lp = None
        self.start_time = 0
        self.memory = 0
        self.cpu = 0
        self.log_error = None
        self.log_info = None
        self.log_traceback = None
        self.log_watch_ts = 0
        self.error_time = 0
        self.errors_today = 0
        self.errors_yesterday = 0
        self.info_time = 0
        self.infos_today = 0
        self.infos_yesterday = 0
        self.traceback_time = 0
        self.tracebacks_today = 0
        self.tracebacks_yesterday = 0
        self.pnl_today = 0
        self.pnl_yesterday = 0
        self.pnl_counter_today = 0
        self.pnl_counter_yesterday = 0
        self.init_found = False

    def watch_log(self):
        yesterday = True
        logfile = Path(f'{self.path}/passivbot.log')
        if logfile.exists():
            seek = False
            if not self.log_lp:
                self.log_lp = 0
                seek = True
            current_position = logfile.stat().st_size
            with open(logfile, "r") as f:
                f.seek(self.log_lp)
                new_content = f.read().splitlines()
            self.log_lp = current_position
            tb_found = False
            today_ts = int(mktime(date.today().timetuple()))
            yesterday_ts = today_ts - 86400
            if self.log_watch_ts != 0 and self.log_watch_ts < today_ts:
                self.log_error = None
                self.log_info = None
                self.log_traceback = None
                self.errors_yesterday = self.errors_today
                self.errors_today = 0
                self.infos_yesterday = self.infos_today
                self.infos_today = 0
                self.tracebacks_yesterday = self.tracebacks_today
                self.tracebacks_today = 0
                self.pnl_yesterday = self.pnl_today
                self.pnl_today = 0
                self.pnl_counter_yesterday = self.pnl_counter_today
                self.pnl_counter_today = 0
            for line in new_content:
                elements = line.split()
                if len(elements) > 1:
                    if elements[1] == "ERROR" or elements[1] == "INFO":
                        # check elements[0] for correct isoformat
                        if len(elements[0]) == 19:
                            if elements[0][4] == "-" and elements[0][7] == "-" and elements[0][10] == "T" and elements[0][13] == ":" and elements[0][16] == ":":
                                ts = int(datetime.fromisoformat(elements[0]).timestamp())
                                if ts < yesterday_ts:
                                    continue
                                else:
                                    seek = False
                                if ts < today_ts:
                                    yesterday = True
                                else:
                                    yesterday = False
                if seek:
                    continue
                if tb_found:
                    if not "ERROR" in line and not "INFO" in line and not "Traceback" in line:
                        self.log_traceback.append(line)
                    else:
                        tb_found = False
                        self.tracebacks_today += 1
                if "ERROR" in line:
                    if yesterday:
                        self.errors_yesterday += 1
                    else:
                        self.log_error = line
                        self.errors_today += 1
                elif "INFO" in line:                 
                    if yesterday:
                        self.infos_yesterday += 1
                    else:
                        self.log_info = line
                        self.infos_today += 1
                    # Skip PNLs after restart bot
                    # Legacy (PB6/PB7 older): "initiating pnl" / "new pnl"
                    # PB7 v7.7+: FillEventsManager + boot banner
                    if (
                        "initiating pnl" in line
                        or "[fills] initializing FillEventsManager" in line
                        or "[boot] starting bot" in line
                    ):
                        self.init_found = True
                    if (
                        "[boot] READY - Bot initialization complete" in line
                        or "starting execution loop" in line
                        or "done initiating bot" in line
                        or "watching" in line
                        or "[fills] initialized" in line
                    ):
                        self.init_found = False
                    if self.init_found:
                        continue

                    # PB7 v7.7+: realized PnL is logged via fill events
                    # Example per fill: "[fill] ... pnl=+5.5 USDT"
                    # Example summary: "[fill] 12 fills, pnl=-1.23 USDT"
                    if "[fill]" in line:
                        match_summary = _PB7_FILL_SUMMARY_RE.search(line)
                        if match_summary:
                            fill_count = int(match_summary.group("count"))
                            pnl_value = float(match_summary.group("pnl"))
                            if yesterday:
                                self.pnl_yesterday += pnl_value
                                self.pnl_counter_yesterday += fill_count
                            else:
                                self.pnl_today += pnl_value
                                self.pnl_counter_today += fill_count
                        else:
                            self_pnl_match = _PB7_FILL_PNL_RE.search(line)
                            if self_pnl_match:
                                pnl_value = float(self_pnl_match.group("pnl"))
                            else:
                                pnl_value = 0.0
                            if yesterday:
                                self.pnl_yesterday += pnl_value
                                self.pnl_counter_yesterday += 1
                            else:
                                self.pnl_today += pnl_value
                                self.pnl_counter_today += 1
                        continue

                    if "new pnl" in line:
                        if len(elements) == 7:
                            if yesterday:
                                self.pnl_yesterday += float(elements[5])
                                self.pnl_counter_yesterday += int(elements[2])
                            else:
                                self.pnl_today += float(elements[5])
                                self.pnl_counter_today += int(elements[2])
                    if "balance" in line:
                        if len(elements) == 6:
                            if elements[4] == "->":
                                if yesterday:
                                    self.pnl_yesterday += (float(elements[5]) - float(elements[3]))
                                    self.pnl_counter_yesterday += 1
                                else:
                                    self.pnl_today += (float(elements[5]) - float(elements[3]))
                                    self.pnl_counter_today += 1
                elif "Traceback" in line:
                    if yesterday:
                        self.tracebacks_yesterday += 1
                    else:
                        self.log_traceback = []
                        self.log_traceback.append(line)
                        tb_found = True
            self.log_watch_ts = int(datetime.now().timestamp())
            self.save_monitor()

    def save_monitor(self):
        monitor_file = Path(f'{self.path}/monitor.json')
        monitor = ({
            # u = user
            # p = pb_version
            # v = version
            # st = start_time
            # m = memory
            # c = cpu
            # i = info
            # it = infos_today
            # iy = infos_yesterday
            # e = error
            # et = errors_today
            # ey = errors_yesterday
            # t = traceback
            # tt = tracebacks_today
            # ty = tracebacks_yesterday
            # pt = pnl_today
            # py = pnl_yesterday
            # ct = pnl_counter_today
            # cy = pnl_counter_yesterday
            "u": self.user,
            "p": self.pb_version,
            "v": self.version,
            "st": self.start_time,
            "m": self.memory,
            "c": self.cpu,
            "i": self.log_info,
            "it": self.infos_today,
            "iy": self.infos_yesterday,
            "e": self.log_error,
            "et": self.errors_today,
            "ey": self.errors_yesterday,
            "t": self.log_traceback,
            "tt": self.tracebacks_today,
            "ty": self.tracebacks_yesterday,
            "pt": self.pnl_today,
            "py": self.pnl_yesterday,
            "ct": self.pnl_counter_today,
            "cy": self.pnl_counter_yesterday
            })
        _atomic_write_json(monitor_file, monitor)

class DynamicIgnore():
    def __init__(self):
        self.path = None
        self.coindata = CoinData()
        self.ignored_coins = []
        self.ignored_coins_long = []
        self.ignored_coins_short = []
        self.approved_coins = []
        self.approved_coins_long = []
        self.approved_coins_short = []

    @staticmethod
    def _normalize_symbol_list(values):
        out = []
        for value in values:
            symbol = str(value or "").strip().upper()
            if symbol:
                out.append(symbol)
        return sorted(set(out))

    @staticmethod
    def _atomic_write_json(path: Path, payload):
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        try:
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f)
            tmp_path.replace(path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            raise

    @staticmethod
    def _is_json_list_file(path: Path) -> bool:
        if not path.exists():
            return False
        try:
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            return isinstance(payload, list)
        except Exception:
            return False

    def lists_ready(self) -> bool:
        if not self.path:
            return False
        ignored_path = Path(f'{self.path}/ignored_coins.json')
        approved_path = Path(f'{self.path}/approved_coins.json')
        return self._is_json_list_file(ignored_path) and self._is_json_list_file(approved_path)

    def list_files_exist(self) -> bool:
        if not self.path:
            return False
        ignored_path = Path(f'{self.path}/ignored_coins.json')
        approved_path = Path(f'{self.path}/approved_coins.json')
        return ignored_path.exists() and approved_path.exists()

    def watch(self):
        try:
            exchange_id = self.coindata.exchange
            available_symbols, _ = self.coindata.filter_mapping(
                exchange=exchange_id,
                market_cap_min_m=0,
                vol_mcap_max=float("inf"),
                only_cpt=False,
                notices_ignore=False,
                tags=[],
                quote_filter=None,
                use_cache=True,
                active_only=True,
            )
            filtered_approved, filtered_ignored = self.coindata.filter_mapping(
                exchange=exchange_id,
                market_cap_min_m=self.coindata.market_cap,
                vol_mcap_max=self.coindata.vol_mcap,
                only_cpt=self.coindata.only_cpt,
                notices_ignore=self.coindata.notices_ignore,
                tags=self.coindata.tags,
                quote_filter=None,
                use_cache=True,
                active_only=True,
            )

            symbol_set = set(self._normalize_symbol_list(available_symbols))

            # Filter-based results are constrained to currently listed symbols.
            # Manual long/short lists are always preserved regardless of listing status
            # (e.g. a delisted coin stays in ignored_coins so it doesn't cause an
            # endless remove/re-add loop when save() re-injects it from ignored_coins_long).
            ignored_from_filter = {
                symbol
                for symbol in self._normalize_symbol_list(list(filtered_ignored))
                if symbol in symbol_set
            }
            manual_ignored = set(self._normalize_symbol_list(
                self.ignored_coins_long + self.ignored_coins_short
            ))
            ignored_coins = sorted(ignored_from_filter | manual_ignored)

            approved_from_filter = {
                symbol
                for symbol in self._normalize_symbol_list(list(filtered_approved))
                if symbol in symbol_set
            }
            manual_approved = set(self._normalize_symbol_list(
                self.approved_coins_long + self.approved_coins_short
            ))
            approved_coins = sorted(approved_from_filter | manual_approved)

            ignored_set = set(ignored_coins)
            approved_coins = [symbol for symbol in approved_coins if symbol not in ignored_set]

            covered = set(approved_coins) | ignored_set
            uncovered = symbol_set - covered
            if uncovered:
                ignored_coins = sorted(ignored_set | uncovered)

            ignored_changed = sorted(self.ignored_coins) != ignored_coins
            approved_changed = sorted(self.approved_coins) != approved_coins

            if ignored_changed:
                removed_coins = sorted(set(self.ignored_coins) - set(ignored_coins))
                added_coins = sorted(set(ignored_coins) - set(self.ignored_coins))
                _log("PBRun", f"Change ignored_coins {self.path} Removed: {removed_coins} Added: {added_coins}")
                self.ignored_coins = ignored_coins

            if approved_changed:
                removed_coins = sorted(set(self.approved_coins) - set(approved_coins))
                added_coins = sorted(set(approved_coins) - set(self.approved_coins))
                _log("PBRun", f"Change approved_coins {self.path} Removed: {removed_coins} Added: {added_coins}")
                self.approved_coins = approved_coins

            if ignored_changed or approved_changed:
                self.save()
                return True
            return False
        except Exception as e:
            _log("PBRun", f"DynamicIgnore watch error for {self.path}: {e}", level="ERROR")
            _log("PBRun", "DynamicIgnore watch traceback", level="DEBUG", meta={"traceback": traceback.format_exc()})
            return False
    
    def save(self):
        if not self.path:
            raise ValueError("DynamicIgnore.path is not set")

        ignored_path = Path(f'{self.path}/ignored_coins.json')
        approved_path = Path(f'{self.path}/approved_coins.json')

        ignored_coins = self._normalize_symbol_list(self.ignored_coins)
        approved_coins = self._normalize_symbol_list(self.approved_coins)

        for symbol in self._normalize_symbol_list(self.ignored_coins_long + self.ignored_coins_short):
            if symbol not in ignored_coins:
                ignored_coins.append(symbol)
            if symbol in approved_coins:
                _log("PBRun", f"Change approved_coins {self.path} Removed: {symbol} because it is in ignored_coins")
                approved_coins.remove(symbol)

        ignored_set = set(ignored_coins)
        for symbol in self._normalize_symbol_list(self.approved_coins_long + self.approved_coins_short):
            if symbol in ignored_set:
                if symbol in approved_coins:
                    _log("PBRun", f"Change approved_coins {self.path} Removed: {symbol} because it is in ignored_coins")
                    approved_coins.remove(symbol)
                continue
            if symbol not in approved_coins:
                approved_coins.append(symbol)

        ignored_coins = self._normalize_symbol_list(ignored_coins)
        approved_coins = self._normalize_symbol_list([symbol for symbol in approved_coins if symbol not in set(ignored_coins)])

        self.ignored_coins = ignored_coins
        self.approved_coins = approved_coins

        self._atomic_write_json(ignored_path, ignored_coins)
        self._atomic_write_json(approved_path, approved_coins)
    

class RunV7():
    def __init__(self):
        self.monitor = Monitor()
        self.user = None
        self.path = None
        self._v7_config = {}
        self.name = None
        self.version = None
        self.pb7dir = None
        self.pb7venv = None
        self.pbgdir = None
        self.dynamic_ignore = None
        self._dynamic_wait_log_ts = 0

    def watch(self):
        if not self.is_running():
            self.start()

    def watch_dynamic(self):
        if self.dynamic_ignore is not None:
            self.dynamic_ignore.watch()

    def is_running(self):
        if self.pid():
            return True
        return False

    def pid(self):
        expected_config = Path(self.path) / "config_run.json"
        for process in psutil.process_iter():
            try:
                cmdline = process.cmdline()
            except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
                continue
            if (
                any("main.py" in sub for sub in cmdline)
                and any(_arg_matches_path(sub, expected_config) for sub in cmdline)
            ):
                _attach_process_stats(process, self.monitor)
                return process

    def stop(self):
        process = self.pid()
        if process:
            _log("PBRun", f"Stop: passivbot v7 {self.path}/config_run.json")
            _kill_process(process, f"v7 {self.path}")
        # Always write 0 — even if bot already crashed and no process found.
        # This ensures running_version.txt reflects "stopped" for inotify → UI.
        version_file = Path(f'{self.path}/running_version.txt')
        _atomic_write_text(version_file, "0")

    def start(self):
        if not self.is_running():
            if self.dynamic_ignore is not None and not _ensure_dynamic_ignore_ready(self.dynamic_ignore):
                now_ts = int(datetime.now().timestamp())
                if now_ts - self._dynamic_wait_log_ts >= 60:
                    _log(
                        "PBRun",
                        f"Delay start: passivbot_v7 {self.path}/config_run.json waiting for dynamic ignore lists",
                        level="WARNING",
                    )
                    self._dynamic_wait_log_ts = now_ts
                return
            self._dynamic_wait_log_ts = 0
            old_os_path = os.environ.get('PATH', '')
            new_os_path = os.path.dirname(self.pb7venv) + os.pathsep + old_os_path
            os.environ['PATH'] = new_os_path
            try:
                cmd = [self.pb7venv, '-u', PurePath(f'{self.pb7dir}/src/main.py'), PurePath(f'{self.path}/config_run.json')]
                logfile = Path(f'{self.path}/passivbot.log')
                with open(logfile, "ab") as log:
                    if platform.system() == "Windows":
                        creationflags = subprocess.DETACHED_PROCESS
                        creationflags |= subprocess.CREATE_NO_WINDOW
                        subprocess.Popen(cmd, stdout=log, stderr=log, cwd=self.pb7dir, text=True, creationflags=creationflags)
                    else:
                        subprocess.Popen(cmd, stdout=log, stderr=log, cwd=self.pb7dir, text=True, start_new_session=True)
            finally:
                os.environ['PATH'] = old_os_path
            _log("PBRun", f"Start: passivbot_v7 {self.path}/config_run.json")
        # wait until passivbot is running
        for i in range(10):
            if self.is_running():
                break
            sleep(1)

    def clean_log(self):
        logfile = Path(f'{self.path}/passivbot.log')
        if logfile.exists():
            if logfile.stat().st_size >= 10485760:
                logfile_old = Path(f'{str(logfile)}.old')
                copy(logfile,logfile_old)
                with open(logfile,'r+') as file:
                    file.truncate()

    def create_v7_running_version(self):
        # Write running Version to file
        version_file = Path(f'{self.path}/running_version.txt')
        _atomic_write_text(version_file, str(self.version))

    def load(self):
        """Load version for PB v7."""
        file = Path(f'{self.path}/config.json')
        file_run = Path(f'{self.path}/config_run.json')
        self.monitor.path = self.path
        self.monitor.user = self.user
        self.monitor.pb_version = "7"
        if file.exists():
            try:
                with open(file, "r", encoding='utf-8') as f:
                    v7_config = f.read()
                self._v7_config = json.loads(v7_config)
                self.version = self._v7_config["pbgui"]["version"]
                self.monitor.version = self.version
                if self.name == self._v7_config["pbgui"]["enabled_on"]:
                    # Fix path in coin_flags
                    if "coin_flags" in self._v7_config["live"]:
                        coin_flags = self._v7_config["live"]["coin_flags"]
                        for coin in coin_flags.copy():
                            flags = coin_flags[coin]
                            if "-lc" in flags:
                                lc = f'-lc {self.path}/{coin}.json'
                                lm = ""
                                lw = ""
                                sm = ""
                                sw = ""
                                lev = ""
                                flags = coin_flags[coin]
                                # if -lm in flags then get mode_long
                                if "-lm" in flags:
                                    lm = f'-lm {flags.split("-lm")[1].split()[0]} '
                                # if -lw in flags then get we_long
                                if "-lw" in flags:
                                    lw = f'-lw {flags.split("-lw")[1].split()[0]} '
                                # if -sm in flags then get mode_short
                                if "-sm" in flags:
                                    sm = f'-sm {flags.split("-sm")[1].split()[0]} '
                                # if -sw in flags then get we_short
                                if "-sw" in flags:
                                    sw = f'-sw {flags.split("-sw")[1].split()[0]} '
                                # if -lev in flags then get leverage
                                if "-lev" in flags:
                                    lev = f'-lev {flags.split("-lev")[1].split()[0]} '
                                new_flags = f"{lm}{lw}{sm}{sw}{lev}{lc}"
                                coin_flags[coin] = new_flags
                        self._v7_config["live"]["coin_flags"] = coin_flags
                        # with open(file_run, "w", encoding='utf-8') as f:
                        #     json.dump(self._v7_config, f, indent=4)
                    if "dynamic_ignore" in self._v7_config["pbgui"]:
                        if self._v7_config["pbgui"]["dynamic_ignore"]:
                            self.dynamic_ignore = DynamicIgnore()
                            self.dynamic_ignore.path = self.path
                            self.dynamic_ignore.coindata.market_cap = self._v7_config["pbgui"]["market_cap"]
                            self.dynamic_ignore.coindata.vol_mcap = self._v7_config["pbgui"]["vol_mcap"]
                            if "only_cpt" in self._v7_config["pbgui"]:
                                self.dynamic_ignore.coindata.only_cpt = self._v7_config["pbgui"]["only_cpt"]
                            if "notices_ignore" in self._v7_config["pbgui"]:
                                self.dynamic_ignore.coindata.notices_ignore = self._v7_config["pbgui"]["notices_ignore"]
                            if "live" in self._v7_config:
                                if "ignored_coins" in self._v7_config["live"]:
                                    if "long" in self._v7_config["live"]["ignored_coins"]:
                                        self.dynamic_ignore.ignored_coins_long = self._v7_config["live"]["ignored_coins"]["long"]
                                    if "short" in self._v7_config["live"]["ignored_coins"]:
                                        self.dynamic_ignore.ignored_coins_short = self._v7_config["live"]["ignored_coins"]["short"]
                                if "approved_coins" in self._v7_config["live"]:
                                    if "long" in self._v7_config["live"]["approved_coins"]:
                                        self.dynamic_ignore.approved_coins_long = self._v7_config["live"]["approved_coins"]["long"]
                                    if "short" in self._v7_config["live"]["approved_coins"]:
                                        self.dynamic_ignore.approved_coins_short = self._v7_config["live"]["approved_coins"]["short"]
                            self._v7_config["live"]["ignored_coins"] = str(PurePath(f'{self.path}/ignored_coins.json'))
                            self._v7_config["live"]["approved_coins"] = str(PurePath(f'{self.path}/approved_coins.json'))
                            # with open(file_run, "w", encoding='utf-8') as f:
                            #     json.dump(self._v7_config, f, indent=4)
                            # Find Exchange from User
                            api_path = f'{self.pb7dir}/api-keys.json'
                            if Path(api_path).exists():
                                with open(api_path, "r", encoding='utf-8') as f:
                                    api_keys = json.load(f)
                                if self.user in api_keys:
                                    self.dynamic_ignore.coindata.exchange = api_keys[self.user]["exchange"]
                                    self.dynamic_ignore.watch()
                    _atomic_write_json(file_run, self._v7_config, indent=4)
                    return True
                else:                        
                    self.name = self._v7_config["pbgui"]["enabled_on"]
                    return False
            except Exception as e:
                _log("PBRun", f"Something went wrong, but continue {e}", level="ERROR")
                _log("PBRun", f"Setting version of {self.user} to 0", level="WARNING")
                self.version = 0
                _log("PBRun", "RunV7.load traceback", level="DEBUG", meta={"traceback": traceback.format_exc()})

class PBRun():
    """PBRun manages v7 passivbot instances, linking PBRemote, PBGui and Passivbot.

    It processes update_status_*.cmd and activate_*.cmd files to install, start and stop v7 bots.

    Robustness notes:
    - Command files are written atomically and malformed files are quarantined after repeated failures.
    - Runtime state files (pid/version/monitor) are written atomically to reduce partial-write corruption.
    """
    def __init__(self):
        # self.run_instances = []
        self.coindata = CoinData()
        self.pbgui_version = "N/A"
        self.pbgui_version_origin = "N/A"
        self.pb7_version = "N/A"
        self.pb7_version_origin = "N/A"
        self.pbgui_commit = "N/A"
        self.pbgui_commit_origin = "N/A"
        self.pb7_commit = "N/A"
        self.pb7_commit_origin = "N/A"
        self.pbgui_python = f"{sys.version_info.major}.{sys.version_info.minor}"
        self.pb7_python = "N/A"
        self._pb7_python_ts = 0
        self.upgrades = 0
        self.reboot = False
        self.run_v7 = []
        self.index = 0
        self.pbgui_branch = "unknown"
        self.pbgui_branches_data = {}
        self.pb7_branch = "unknown"
        self.pb7_branches_data = {}
        self.pbgdir = Path.cwd()
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        # Init activate_ts and pbname
        if pb_config.has_option("main", "pbname"):
            self.name = pb_config.get("main", "pbname")
        else:
            self.name = platform.node()
        if pb_config.has_option("main", "activate_v7_ts"):
            self.activate_v7_ts = int(pb_config.get("main", "activate_v7_ts"))
        else:
            self.activate_v7_ts = 0
        self.instances_status_v7 = InstancesStatus(f'{self.pbgdir}/data/cmd/status_v7.json')
        self.instances_status_v7.pbname = self.name
        self.instances_status_v7.activate_ts = self.activate_v7_ts
        # Init pbdirs
        self.pb7dir = None
        if pb_config.has_option("main", "pb7dir"):
            self.pb7dir = pb_config.get("main", "pb7dir")
        if not self.pb7dir:
            if __name__ == '__main__':
                _log("PBRun", "No passivbot directory configured in pbgui.ini", level="ERROR")
                sys.exit(1)
            else:
                _log("PBRun", "No passivbot directory configured in pbgui.ini", level="ERROR")
                return
        # Init pbvenvs
        self.pb7venv = None
        if pb_config.has_option("main", "pb7venv"):
            self.pb7venv = pb_config.get("main", "pb7venv")
        if not self.pb7venv:
            if __name__ == '__main__':
                _log("PBRun", "No passivbot venv python interpreter configured in pbgui.ini", level="ERROR")
                sys.exit(1)
            else:
                _log("PBRun", "No passivbot venv python interpreter configured in pbgui.ini", level="ERROR")
                return
        # Init paths
        self.v7_path = f'{self.pbgdir}/data/run_v7'
        self.cmd_path = f'{self.pbgdir}/data/cmd'
        if not Path(self.cmd_path).exists():
            Path(self.cmd_path).mkdir(parents=True)            
        self.failed_cmd_path = Path(f'{self.cmd_path}/failed')
        self.failed_cmd_path.mkdir(parents=True, exist_ok=True)
        self._bad_cmd_failures = {}
        self._bad_cmd_quarantine_after = 3
        # Init pid
        self.piddir = Path(f'{self.pbgdir}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbrun.pid')
        self.my_pid = None

    def _handle_bad_cmd_file(self, cfile: Path, command_kind: str, error: Exception):
        key = str(cfile)
        failures = self._bad_cmd_failures.get(key, 0) + 1
        self._bad_cmd_failures[key] = failures
        _log(
            "PBRun",
            f"Invalid {command_kind} command file {cfile}: {error} (attempt {failures}/{self._bad_cmd_quarantine_after})",
            level="WARNING",
        )
        if failures < self._bad_cmd_quarantine_after:
            return
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        quarantine_name = f"{cfile.stem}.failed_{ts}{cfile.suffix}"
        quarantine_path = self.failed_cmd_path / quarantine_name
        try:
            cfile.replace(quarantine_path)
            _log("PBRun", f"Quarantined invalid command file {cfile} -> {quarantine_path}", level="WARNING")
        except Exception as qerr:
            _log("PBRun", f"Failed to quarantine invalid command file {cfile}: {qerr}", level="ERROR")
        finally:
            self._bad_cmd_failures.pop(key, None)

    def update_pb7_python_version(self, force: bool = False):
        """Cache PB7 venv Python major.minor (e.g. 3.12) for status display."""
        timestamp = round(datetime.now().timestamp())
        if not force and self._pb7_python_ts and (timestamp - self._pb7_python_ts) < 3600:
            return
        self._pb7_python_ts = timestamp
        self.pb7_python = "N/A"
        if not self.pb7venv:
            return
        try:
            if not Path(self.pb7venv).exists():
                return
            cmd = [
                self.pb7venv,
                "-c",
                "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')",
            ]
            res = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=5,
            )
            if res.returncode == 0:
                out = (res.stdout or "").strip()
                if out:
                    self.pb7_python = out
        except Exception:
            return

    def has_upgrades(self):
        """Check if apt-get dist-upgrade -s finds upgrades available"""
        my_env = os.environ.copy()
        my_env["LANG"] = 'C'
        apt_upgrade = _run_subprocess(["apt-get", "dist-upgrade", "-s"], env=my_env, timeout=15)
        if not apt_upgrade or apt_upgrade.returncode != 0:
            self.upgrades = "N/A"
            return
        match = re.search(r"(\d+) upgraded", apt_upgrade.stdout)
        if match:
            self.upgrades = match.group(1)

    def has_reboot(self):
        """Check if /var/run/reboot-required exists"""
        if Path("/var/run/reboot-required").exists():
            self.reboot = True

    def load_git_origin(self):
        """git fetch origin and load last commit from origin/master"""
        pbgui_git = Path(f'{self.pbgdir}/.git')
        if pbgui_git.exists():
            pbgui_git = Path(f'{self.pbgdir}/.git')
            _run_subprocess(["git", "--git-dir", f'{pbgui_git}', "fetch", "origin"], timeout=30)
            pbgui_commit = _run_subprocess(["git", "--git-dir", f'{pbgui_git}', "log", "-n", "1", "--pretty=format:%H", "origin/main"], timeout=15)
            if pbgui_commit and pbgui_commit.returncode == 0:
                self.pbgui_commit_origin = pbgui_commit.stdout
        if self.pb7dir:
            pb7_git = Path(f'{self.pb7dir}/.git')
            if pb7_git.exists():
                pb7_git = Path(f'{self.pb7dir}/.git')
                _run_subprocess(["git", "--git-dir", f'{pb7_git}', "fetch", "origin"], timeout=30)
                pb7_commit = _run_subprocess(["git", "--git-dir", f'{pb7_git}', "log", "-n", "1", "--pretty=format:%H", "origin/master"], timeout=15)
                if pb7_commit and pb7_commit.returncode == 0:
                    self.pb7_commit_origin = pb7_commit.stdout

    def load_git_commits(self):
        """Load the git commit hash of pbgui, pb6 and pb7 using git log -n 1"""
        pbgui_git = Path(f'{self.pbgdir}/.git')
        if pbgui_git.exists():
            pbgui_git = Path(f'{self.pbgdir}/.git')
            # Get full commit hash
            pbgui_commit = _run_subprocess(["git", "--git-dir", f'{pbgui_git}', "log", "-n", "1", "--pretty=format:%H"], timeout=15)
            if pbgui_commit and pbgui_commit.returncode == 0:
                self.pbgui_commit = pbgui_commit.stdout
            # Get current branch
            pbgui_branch = _run_subprocess(["git", "--git-dir", f'{pbgui_git}', "rev-parse", "--abbrev-ref", "HEAD"], timeout=15)
            if pbgui_branch and pbgui_branch.returncode == 0:
                self.pbgui_branch = pbgui_branch.stdout.strip()
        if self.pb7dir:
            pb7_git = Path(f'{self.pb7dir}/.git')
            if pb7_git.exists():
                pb7_git = Path(f'{self.pb7dir}/.git')
                pb7_commit = _run_subprocess(["git", "--git-dir", f'{pb7_git}', "log", "-n", "1", "--pretty=format:%H"], timeout=15)
                if pb7_commit and pb7_commit.returncode == 0:
                    self.pb7_commit = pb7_commit.stdout
                # Get current branch
                pb7_branch = _run_subprocess(["git", "--git-dir", f'{pb7_git}', "rev-parse", "--abbrev-ref", "HEAD"], timeout=15)
                if pb7_branch and pb7_branch.returncode == 0:
                    self.pb7_branch = pb7_branch.stdout.strip()

    def load_git_branches_history(self):
        """Load commit history for all branches (last 50 commits per branch)"""
        pbgui_git = Path(f'{self.pbgdir}/.git')
        if not pbgui_git.exists():
            return
        
        # Fetch latest changes from remote first
        _run_subprocess(
            ["git", "--git-dir", f'{pbgui_git}', "fetch", "origin"],
            timeout=30,
            capture_stdout=False,
            suppress_stderr=True,
        )
        
        # Get all branches (local and remote)
        branches_result = _run_subprocess(
            ["git", "--git-dir", f'{pbgui_git}', "branch", "-a"],
            timeout=15,
            suppress_stderr=True,
        )
        if not branches_result or branches_result.returncode != 0:
            return
        
        branches_data = {}
        # First pass: collect remote branch names to prioritize them
        remote_branches = set()
        for line in branches_result.stdout.splitlines():
            branch_raw = line.strip().lstrip('* ')
            if branch_raw.startswith('remotes/origin/') and 'HEAD ->' not in branch_raw:
                branch_name = branch_raw.replace('remotes/origin/', '')
                remote_branches.add(branch_name)
        
        # Second pass: process branches, preferring remote over local
        for line in branches_result.stdout.splitlines():
            # Clean branch name (remove * and whitespace)
            branch_raw = line.strip().lstrip('* ')
            if not branch_raw or 'HEAD ->' in branch_raw:
                continue
            
            # For remote branches, use the full remotes/origin/xxx format
            # For local branches, use as-is BUT skip if remote exists
            if branch_raw.startswith('remotes/origin/'):
                branch_ref = branch_raw  # Keep full path for git log
                branch_name = branch_raw.replace('remotes/origin/', '')  # Display name
            else:
                branch_name = branch_raw
                # Skip local branch if remote version exists (remote is more current after fetch)
                if branch_name in remote_branches:
                    continue
                branch_ref = branch_raw
            
            # Skip if already processed
            if branch_name in branches_data:
                continue
            
            # Get last 50 commits for this branch using proper reference
            commits_result = _run_subprocess(
                ["git", "--git-dir", f'{pbgui_git}', "log", branch_ref, "-n", "50",
                 "--pretty=format:%h|%H|%an|%ar|%at|%B%x00"],
                timeout=20,
                suppress_stderr=True,
            )
            if not commits_result:
                continue
            
            # Skip if git log failed (branch doesn't exist locally)
            if commits_result.returncode != 0:
                continue
            
            commits, latest_commit_timestamp = _parse_git_log_output(
                commits_result.stdout,
                f"branch {branch_name}",
            )
            
            if commits:
                _log("PBRun", f"Loaded {len(commits)} commits for branch {branch_name}")
                branches_data[branch_name] = {
                    'commits': commits,
                    'latest_timestamp': latest_commit_timestamp
                }
        
        # Sort branches by latest commit timestamp (newest first)
        sorted_branches = dict(sorted(branches_data.items(), 
                                     key=lambda x: x[1]['latest_timestamp'] if x[1]['latest_timestamp'] else 0, 
                                     reverse=True))
        
        # Convert back to simple format (just commits list) for backward compatibility
        self.pbgui_branches_data = {name: data['commits'] for name, data in sorted_branches.items()}

    def load_more_commits(self, branch_name: str, limit: int):
        """Load more commits for a specific branch
        
        Args:
            branch_name: Name of the branch to load commits for
            limit: Total number of commits to load
        """
        pbgui_git = Path(f'{self.pbgdir}/.git')
        if not pbgui_git.exists():
            return
        
        # Fetch latest changes from remote first
        _run_subprocess(
            ["git", "--git-dir", f'{pbgui_git}', "fetch", "origin"],
            timeout=30,
            capture_stdout=False,
            suppress_stderr=True,
        )
        
        # Determine branch reference
        if branch_name in self.pbgui_branches_data:
            # Branch already exists, use appropriate reference
            branch_ref = f"remotes/origin/{branch_name}" if branch_name != self.pbgui_branch else branch_name
        else:
            return
        
        # Build git log command
        cmd = ["git", "--git-dir", f'{pbgui_git}', "log", branch_ref, "-n", str(limit), "--pretty=format:%h|%H|%an|%ar|%at|%B%x00"]
        
        commits_result = _run_subprocess(
            cmd,
            timeout=20,
            suppress_stderr=True,
        )
        if not commits_result:
            return
        
        if commits_result.returncode != 0:
            return
        
        commits, _ = _parse_git_log_output(commits_result.stdout, f"branch {branch_name}")
        
        if commits:
            _log("PBRun", f"Loaded {len(commits)} commits for branch {branch_name}")
            self.pbgui_branches_data[branch_name] = commits

    def get_current_pb7_status(self):
        """Get current PB7 branch and commit directly from git (live query)"""
        if not self.pb7dir:
            return None, None
        
        pb7_git = Path(f'{self.pb7dir}/.git')
        if not pb7_git.exists():
            return None, None
        
        # Get current commit
        commit_result = _run_subprocess(
            ["git", "--git-dir", f'{pb7_git}', "rev-parse", "HEAD"],
            timeout=15,
            suppress_stderr=True,
        )
        current_commit = commit_result.stdout.strip() if commit_result and commit_result.returncode == 0 else ""
        
        # Get current branch (works reliably since we use git reset --hard)
        branch_result = _run_subprocess(
            ["git", "--git-dir", f'{pb7_git}', "symbolic-ref", "--short", "HEAD"],
            timeout=15,
            suppress_stderr=True,
        )
        current_branch = branch_result.stdout.strip() if branch_result and branch_result.returncode == 0 else "unknown"
        
        return current_branch, current_commit

    def get_current_pbgui_status(self):
        """Get current PBGui branch and commit directly from git (live query)"""
        if not self.pbgdir:
            return None, None
            
        pbgui_git = Path(f'{self.pbgdir}/.git')
        if not pbgui_git.exists():
            return None, None
        
        # Get current commit
        commit_result = _run_subprocess(
            ["git", "--git-dir", f'{pbgui_git}', "rev-parse", "HEAD"],
            timeout=15,
            suppress_stderr=True,
        )
        current_commit = commit_result.stdout.strip() if commit_result and commit_result.returncode == 0 else ""
        
        # Get current branch (works reliably since we use git reset --hard)
        branch_result = _run_subprocess(
            ["git", "--git-dir", f'{pbgui_git}', "symbolic-ref", "--short", "HEAD"],
            timeout=15,
            suppress_stderr=True,
        )
        current_branch = branch_result.stdout.strip() if branch_result and branch_result.returncode == 0 else "unknown"
        
        return current_branch, current_commit

    def load_pb7_branches_history(self):
        """Load commit history for all PB7 branches (last 50 commits per branch)"""
        if not self.pb7dir:
            return
        
        pb7_git = Path(f'{self.pb7dir}/.git')
        if not pb7_git.exists():
            return
        
        # Fetch latest changes from remote first
        _run_subprocess(
            ["git", "--git-dir", f'{pb7_git}', "fetch", "origin"],
            timeout=30,
            capture_stdout=False,
            suppress_stderr=True,
        )
        
        # Get all branches (local and remote)
        branches_result = _run_subprocess(
            ["git", "--git-dir", f'{pb7_git}', "branch", "-a"],
            timeout=15,
            suppress_stderr=True,
        )
        if not branches_result or branches_result.returncode != 0:
            return
        
        branches_data = {}
        # First pass: collect remote branch names to prioritize them
        remote_branches = set()
        for line in branches_result.stdout.splitlines():
            branch_raw = line.strip().lstrip('* ')
            if branch_raw.startswith('remotes/origin/') and 'HEAD ->' not in branch_raw:
                branch_name = branch_raw.replace('remotes/origin/', '')
                remote_branches.add(branch_name)
        
        # Second pass: process branches, preferring remote over local
        for line in branches_result.stdout.splitlines():
            # Clean branch name (remove * and whitespace)
            branch_raw = line.strip().lstrip('* ')
            if not branch_raw or 'HEAD ->' in branch_raw:
                continue
            
            # For remote branches, use the full remotes/origin/xxx format
            # For local branches, use as-is BUT skip if remote exists
            if branch_raw.startswith('remotes/origin/'):
                branch_ref = branch_raw  # Keep full path for git log
                branch_name = branch_raw.replace('remotes/origin/', '')  # Display name
            else:
                branch_name = branch_raw
                # Skip local branch if remote version exists (remote is more current after fetch)
                if branch_name in remote_branches:
                    continue
                branch_ref = branch_raw
            
            # Skip if already processed
            if branch_name in branches_data:
                continue
            
            # Filter: Only include v7 relevant branches
            # Strategy: Include master and v7.x branches always
            # For other branches: check commit date (v7.0.0 was released around mid-2023)
            # Skip old version branches explicitly (v6, v5, v4, v3, v2, v1, v0)
            skip_patterns = ['v6.', 'v5.', 'v4.', 'v3.', 'v2.', 'v1.', 'v0.', 'release/v6', 'release/v1', 'release/v0']
            if any(branch_name.startswith(pattern) for pattern in skip_patterns):
                continue
            
            # Include master and v7 branches always
            include_always = (branch_name == 'master' or 
                            branch_name.startswith('v7.') or 
                            branch_name.startswith('v7-'))
            
            # Get last 50 commits for this branch using proper reference
            commits_result = _run_subprocess(
                ["git", "--git-dir", f'{pb7_git}', "log", branch_ref, "-n", "50",
                 "--pretty=format:%h|%H|%an|%ar|%at|%B%x00"],
                timeout=20,
                suppress_stderr=True,
            )
            if not commits_result:
                continue
            
            # Skip if git log failed (branch doesn't exist locally)
            if commits_result.returncode != 0:
                continue
            
            commits, latest_commit_timestamp = _parse_git_log_output(
                commits_result.stdout,
                f"PB7 branch {branch_name}",
            )
            
            # Filter by date: v7.0.0 was released around June 2023 (timestamp ~1686000000)
            # Only include branches if: master/v7.x OR latest commit is after June 2023
            v7_release_timestamp = 1686000000  # ~June 2023
            if not include_always:
                if not latest_commit_timestamp or latest_commit_timestamp < v7_release_timestamp:
                    # Branch is too old, skip it
                    continue
            
            if commits:
                _log("PBRun", f"Loaded {len(commits)} commits for PB7 branch {branch_name}")
                # Store commits with branch metadata (latest timestamp for sorting)
                branches_data[branch_name] = {
                    'commits': commits,
                    'latest_timestamp': latest_commit_timestamp
                }
        
        # Sort branches by latest commit timestamp (newest first)
        sorted_branches = dict(sorted(branches_data.items(), 
                                     key=lambda x: x[1]['latest_timestamp'] if x[1]['latest_timestamp'] else 0, 
                                     reverse=True))
        
        # Convert back to simple format (just commits list)
        self.pb7_branches_data = {name: data['commits'] for name, data in sorted_branches.items()}

    def load_more_pb7_commits(self, branch_name: str, limit: int):
        """Load more commits for a specific PB7 branch
        
        Args:
            branch_name: Name of the branch to load commits for
            limit: Total number of commits to load
        """
        if not self.pb7dir:
            return
            
        pb7_git = Path(f'{self.pb7dir}/.git')
        if not pb7_git.exists():
            return
        
        # Fetch latest changes from remote first
        _run_subprocess(
            ["git", "--git-dir", f'{pb7_git}', "fetch", "origin"],
            timeout=30,
            capture_stdout=False,
            suppress_stderr=True,
        )
        
        # Determine branch reference
        if branch_name in self.pb7_branches_data:
            # Branch already exists, use appropriate reference
            branch_ref = f"remotes/origin/{branch_name}" if branch_name != self.pb7_branch else branch_name
        else:
            return
        
        # Build git log command (include timestamp for consistency)
        cmd = ["git", "--git-dir", f'{pb7_git}', "log", branch_ref, "-n", str(limit), "--pretty=format:%h|%H|%an|%ar|%at|%B%x00"]
        
        commits_result = _run_subprocess(
            cmd,
            timeout=20,
            suppress_stderr=True,
        )
        if not commits_result:
            return
        
        if commits_result.returncode != 0:
            return
        
        commits, _ = _parse_git_log_output(commits_result.stdout, f"PB7 branch {branch_name}")
        
        if commits:
            _log("PBRun", f"Loaded {len(commits)} commits for PB7 branch {branch_name}")
            self.pb7_branches_data[branch_name] = commits

    def load_versions_origin(self):
        """git show origin:README.md and load the versions of pbgui, pb6 and pb7"""
        if Path(f'{self.pbgdir}/.git').exists():
            pbgui_readme_origin = _run_subprocess(["git", "--git-dir", f'{self.pbgdir}/.git', "show", "origin/main:README.md"], timeout=20)
            if not pbgui_readme_origin or pbgui_readme_origin.returncode != 0:
                pbgui_readme_origin = None
            lines = pbgui_readme_origin.stdout.splitlines() if pbgui_readme_origin else []
            for line in lines:
                #find regex regex_search('^#? ?v[0-9.]+'
                version = re.search('v[0-9.]+', line)
                if version:
                    self.pbgui_version_origin = version.group(0)
                    break
        if Path(f'{self.pb7dir}/.git').exists():
            pb7_vfile_origin = _run_subprocess(["git", "--git-dir", f'{self.pb7dir}/.git', "show", "origin/master:src/passivbot_version.py"], timeout=20)
            if pb7_vfile_origin and pb7_vfile_origin.returncode == 0:
                m = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', pb7_vfile_origin.stdout)
                if m:
                    self.pb7_version_origin = f'v{m.group(1)}'

    def load_versions(self):
        """Load the versions of pbgui, pb6 and pb7 from README.md"""
        pbgui_readme = Path(f'{self.pbgdir}/README.md')
        if pbgui_readme.exists():
            # read only first 20 lines
            with open(pbgui_readme, "r", encoding='utf-8') as f:
                lines = f.readlines()[:20]
            for line in lines:
                #find regex regex_search('^#? ?v[0-9.]+'
                version = re.search('v[0-9.]+', line)
                if version:
                    self.pbgui_version = version.group(0)
                    break
        if self.pb7dir:
            pb7_vfile = Path(f'{self.pb7dir}/src/passivbot_version.py')
            if pb7_vfile.exists():
                with open(pb7_vfile, "r", encoding='utf-8') as f:
                    content = f.read()
                m = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
                if m:
                    self.pb7_version = f'v{m.group(1)}'

    def fetch_cmc_credits(self):
        self.coindata.fetch_api_status()        

    def add_v7(self, run_v7: RunV7):
        if run_v7:
            for v7 in self.run_v7:
                if v7.path == run_v7.path:
                    self.run_v7.remove(v7)
                    self.run_v7.append(run_v7)
                    # v7.version = run_v7.version
                    return
            self.run_v7.append(run_v7)
    
    def remove_v7(self, run_v7: RunV7):
        if run_v7:
            for v7 in self.run_v7:
                if v7.path == run_v7.path:
                    self.run_v7.remove(v7)
                    return

    def find_running_version(self, path: str):
        version = 0
        version_file = Path(f'{path}/running_version.txt')
        if version_file.exists():
            with open(version_file, "r", encoding='utf-8') as f:
                version = f.read()
        return int(version)

    def update_status(self, status_file : str, rserver : str):
        """Function only called on PBRemote"""
        unique = str(uuid.uuid4())
        cfile = Path(f'{self.cmd_path}/update_status_{unique}.cmd')
        cfg = ({
            "rserver": rserver,
            "status_file": str(status_file)})
        _atomic_write_json(cfile, cfg)

    def has_update_status(self):
        """Checks for update_status_*.cmd files and applies v7 status updates."""
        p = str(Path(f'{self.cmd_path}/update_status_*.cmd'))
        status_files = glob.glob(p)
        for cfile in status_files:
            cfile = Path(cfile)
            if cfile.exists():
                try:
                    with open(cfile, "r", encoding='utf-8') as f:
                        cfg = json.load(f)
                    rserver = cfg["rserver"]
                    status_file = cfg["status_file"]
                    if status_file.split('/')[-1] == 'status_v7.json':
                        self.update_from_status_v7(status_file, rserver)
                    cfile.unlink(missing_ok=True)
                    self._bad_cmd_failures.pop(str(cfile), None)
                except Exception as e:
                    self._handle_bad_cmd_file(cfile, "update status", e)

    def update_from_status_v7(self, status_file : str, rserver : str):
        """Updates the v7 status based on the provided status file.

        Notes:
            - Compares per-instance activate_ts to detect changes.
            - Installs new v7 versions or instances if their activate_ts is newer.
            - Removes old *.json configuration files.
            - Removes instances not found in the new status.

        Args:
            status_file (str): Path to the status file.
            rserver (str): Name of the remote server.
        """
        new_status = InstancesStatus(status_file)
        _log("PBRun", f"Received v7 status from {new_status.activate_pbname}")
        changed = False
        for instance in new_status:
            local = self.instances_status_v7.find_name(instance.name)
            if local is not None:
                # Per-instance activate_ts comparison
                if instance.activate_ts <= local.activate_ts:
                    continue
                if instance.version > local.version:
                    # Backup old v7 config
                    source = f'{self.v7_path}/{instance.name}'
                    if Path(source).exists():
                        destination = Path(f'{self.pbgdir}/data/backup/v7/{instance.name}/{local.version}')
                        if not destination.exists():
                            destination.mkdir(parents=True)
                        copytree(source, destination, dirs_exist_ok=True, ignore=ignore_patterns('passivbot.log', 'passivbot.log.old', 'ignored_coins.json', 'approved_coins.json', 'config_run.json', 'monitor.json'))
                    # Install new v7 version
                    _log("PBRun", f"Install: New V7 Version {instance.name} Old: {local.version} New: {instance.version}")
                    # Remove old *.json configs
                    dest = f'{self.v7_path}/{instance.name}'
                    p = str(Path(f'{dest}/*'))
                    items = glob.glob(p)
                    for item in items:
                        if item.endswith('.json'):
                            Path(item).unlink(missing_ok=True)
                    src = f'{self.pbgdir}/data/remote/run_v7_{rserver}/{instance.name}'
                    dest = f'{self.v7_path}/{instance.name}'
                    if Path(src).exists():
                        copytree(src, dest, dirs_exist_ok=True)
                        self.watch_v7([f'{self.v7_path}/{instance.name}'])
                    changed = True
                # Update local activate_ts even if version unchanged (enabled_on etc. may differ)
                local.activate_ts = instance.activate_ts
                local.enabled_on = instance.enabled_on
                changed = True
            else:
                # Install new v7 instance
                _log("PBRun", f"Install: New V7 Instance {instance.name} from {rserver} Version: {instance.version}")
                src = f'{self.pbgdir}/data/remote/run_v7_{rserver}/{instance.name}'
                dest = f'{self.v7_path}/{instance.name}'
                if Path(src).exists():
                    copytree(src, dest, dirs_exist_ok=True)
                    self.watch_v7([f'{self.v7_path}/{instance.name}'])
                changed = True
        remove_instances = []
        for instance in self.instances_status_v7:
            status = new_status.find_name(instance.name)
            if status is None:
                # Remove v7 instance
                _log("PBRun", f"Remove: V7 Instance {instance.name}")
                if instance.running:
                    for v7 in self.run_v7:
                        name = v7.path.split('/')[-1]
                        if name == instance.name:
                            v7.stop()
                            self.remove_v7(v7)
                source = f'{self.v7_path}/{instance.name}'
                if Path(source).exists():
                    # Backup v7 config
                    date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                    destination = Path(f'{self.pbgdir}/data/backup/v7/{instance.name}/{date}')
                    if not destination.exists():
                        destination.mkdir(parents=True)
                    copytree(source, destination, dirs_exist_ok=True, ignore=ignore_patterns('passivbot.log', 'passivbot.log.old', 'ignored_coins.json', 'approved_coins.json', 'config_run.json', 'monitor.json'))
                    rmtree(source, ignore_errors=True)
                    remove_instances.append(instance)
        if remove_instances:
            for instance in remove_instances:
                self.instances_status_v7.remove(instance)
            changed = True

    def has_status_v7_changed(self):
        """Poll status_v7.json for mtime changes and rescan v7 instances."""
        if self.instances_status_v7.has_new_status():
            _log("PBRun", "status_v7.json changed — rescanning v7 instances")
            self.watch_v7()
    
    def update_activate_v7(self):
        self._update_activate_timestamp("activate_v7_ts", "instances_status_v7")

    def _update_activate_timestamp(self, key: str, status_attr: str):
        now_ts = int(datetime.now().timestamp())
        if key == "activate_v7_ts":
            self.activate_v7_ts = now_ts

        status = getattr(self, status_attr, None)
        if status is not None:
            status.activate_ts = now_ts

        pb_config = configparser.ConfigParser()
        pb_config.read("pbgui.ini")
        if not pb_config.has_section("main"):
            pb_config.add_section("main")
        pb_config.set("main", key, str(now_ts))
        _atomic_write_config(Path("pbgui.ini"), pb_config)

    def watch_v7(self, v7_instances : list = None):
        """Create or delete v7 instances and activate them or not depending on their status.

        Args:
            v7_instances (list, optional): List of v7-instance paths. Defaults to None.
        """
        # Preserve per-instance activate_ts before clearing
        old_ts = {}
        for inst in self.instances_status_v7:
            if inst.activate_ts:
                old_ts[inst.name] = inst.activate_ts
        if not v7_instances:
            p = str(Path(f'{self.v7_path}/*'))
            v7_instances = glob.glob(p)
            # Remove all existing instances from status
            self.instances_status_v7.instances = []
        for v7_instance in v7_instances:
            file = Path(f'{v7_instance}/config.json')
            if file.exists():
                run_v7 = RunV7()
                status = InstanceStatus()
                run_v7.path = v7_instance
                run_v7.user = v7_instance.split('/')[-1]
                status.name = run_v7.user
                run_v7.name = self.name
                run_v7.pb7dir = self.pb7dir
                run_v7.pb7venv = self.pb7venv
                run_v7.pbgdir = self.pbgdir
                if run_v7.load():
                    if run_v7.is_running():
                        running_version = self.find_running_version(v7_instance)
                        if running_version < run_v7.version:
                            run_v7.stop()
                            run_v7.create_v7_running_version()
                            run_v7.start()
                    else:
                        run_v7.create_v7_running_version()
                        run_v7.start()
                    self.add_v7(run_v7)
                    status.running = True
                else:
                    self.remove_v7(run_v7)
                    status.running = False
                    run_v7.stop()
                status.version = run_v7.version
                status.enabled_on = run_v7.name
                # Restore per-instance activate_ts
                status.activate_ts = old_ts.get(status.name, 0)
                self.instances_status_v7.add(status)
        # Remove non existing instances from status
        for instance in self.instances_status_v7:
            instance_path = f'{self.pbgdir}/data/run_v7/{instance.name}'
            if not Path(instance_path).exists():
                self.instances_status_v7.remove(instance)

    def find_high_memory_bot(self):
        """Finds the bot with the highest memory usage."""
        high_mem = 0
        high_bot = None
        for v7 in self.run_v7:
            mem = _memory_usage_bytes(v7.monitor.memory)
            if mem > high_mem:
                high_mem = mem
                high_bot = v7
        return high_bot
    
    def watch_memory(self):
        """Watches the memory usage of the System and restart Passivbot if necessary."""
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        free = (mem.available + swap.free) / 1024 / 1024  # in MB
        if free < 250:
            high_bot = self.find_high_memory_bot()
            if high_bot:
                _log("PBRun", f"Low System memory {free:.2f}MB, restarting bot {high_bot.user}", level="WARNING")
                high_bot.stop()
                high_bot.start()

    def run(self):
        if not self.is_running():
            pbgdir = Path.cwd()
            cmd = [sys.executable, '-u', PurePath(f'{pbgdir}/PBRun.py')]
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, start_new_session=True)
            count = 0
            while True:
                if count > 5:
                    _log("PBRun", "Can not start PBRun", level="ERROR")
                    break
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            _log("PBRun", "Stop: PBRun")
            try:
                _kill_process(psutil.Process(self.my_pid), "PBRun")
            except psutil.NoSuchProcess:
                pass

    def restart_pbrun(self):
        if self.is_running():
            self.stop()
            self.run()

    def is_running(self):
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(sub.lower().endswith("pbrun.py") for sub in psutil.Process(self.my_pid).cmdline()):
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
        """Saves the process ID into /data/pid/pbrun.pid."""
        self.my_pid = os.getpid()
        _atomic_write_text(self.pidfile, str(self.my_pid))


def main():
    """
    Main function of PBRun, responsible for starting and monitoring v7 passivbot instances.
    """
    run = PBRun()
    if run.is_running():
        _log("PBRun", "PBRun already started", level="ERROR")
        sys.exit(1)
    _log("PBRun", "Start: PBRun")
    run.save_pid()
    if run.pb7dir:
        run.watch_v7()
    count = 0
    while True:
        try:
            run.watch_memory()
            run.has_update_status()
            if run.pb7dir:
                run.has_status_v7_changed()
                for run_v7 in run.run_v7:
                    run_v7.watch()
                    run_v7.watch_dynamic()
                    run_v7.monitor.watch_log()
            if count%2 == 0:
                if run.pb7dir:
                    for run_v7 in run.run_v7:
                        run_v7.clean_log()
            sleep(5)
            count += 1
        except Exception as e:
            _log("PBRun", f"Something went wrong, but continue {e}", level="ERROR")
            _log("PBRun", "PBRun.main loop traceback", level="DEBUG", meta={"traceback": traceback.format_exc()})

if __name__ == '__main__':
    main()