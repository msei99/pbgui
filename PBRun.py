"""
PBRun manages v7 passivbot instances.

It checks for status and activate files, starts and stops v7 bots accordingly.
"""
import psutil
import subprocess
import threading
import configparser
import shlex
import sys
from pathlib import Path, PurePath
from time import sleep, time
import glob
import json
import hjson
import re
from datetime import datetime, timedelta
import platform
from shutil import copy, copytree, rmtree, ignore_patterns
import os
import traceback
import uuid
import copy as copy_module
from Status import InstanceStatus, InstancesStatus
from PBCoinData import CoinData
from logging_helpers import human_log as _log, get_rotate_settings, rotate_logfile_if_oversize
from master.cluster_state import (
    build_config_manifest,
    compute_config_manifest_hash,
    default_cluster_root,
    read_local_identity,
)


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


def _attach_process_stats(process: psutil.Process, run_v7: "RunV7"):
    run_v7.start_time = process.create_time()
    try:
        run_v7.memory = process.memory_full_info()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        run_v7.memory = None
    try:
        run_v7.cpu = process.cpu_percent()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        run_v7.cpu = None


def _ts_wrap_stderr(stderr_pipe, filepath: str):
    """Read stderr lines and write non-log lines with UTC timestamp prefix.

    Passivbot log lines already have a ``YYYY-MM-DDTHH:MM:SSZ`` prefix and are
    written to ``pb7/logs/`` by passivbot's own file handler — skip those.
    Only raw stderr (tracebacks, startup tracebacks, subprocess noise) gets saved.
    """
    try:
        with open(filepath, "ab") as f:
            for raw_line in stderr_pipe:
                line = raw_line.strip() if isinstance(raw_line, str) else raw_line.decode(errors='replace').strip()
                if not line:
                    continue
                # skip passivbot log lines (already saved via its FileHandler)
                if len(line) >= 20 and line[4] == '-' and line[7] == '-' and line[10] == 'T' and line[19] == 'Z':
                    continue
                ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                f.write(f"{ts} {line}\n".encode())
    except Exception:
        pass


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


def _read_json_file(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload if isinstance(payload, dict) else {}


def _cluster_gate_is_configured(cluster_root: Path) -> bool:
    if not cluster_root.exists():
        return False
    markers = (
        "cluster_id",
        "node_id",
        "node_identity.json",
        "cluster_nodes.json",
        "desired_state.json",
        "state_vector.json",
    )
    return any((cluster_root / marker).exists() for marker in markers)


def _wait_for_cluster_boot_sync(pbgdir: Path, *, timeout: int = 20) -> dict:
    """Wait briefly for PBCluster boot sync without making stale state fatal."""

    cluster_root = default_cluster_root(Path(pbgdir))
    if not _cluster_gate_is_configured(cluster_root):
        return {"status": "not_configured", "waited": 0}

    status_path = cluster_root / "sync_status.json"
    started_at = int(time())
    deadline = time() + max(0, int(timeout))
    last_status: dict = {}
    first_check = True
    while first_check or time() <= deadline:
        first_check = False
        if status_path.is_file():
            try:
                payload = _read_json_file(status_path)
            except Exception:
                payload = {}
            if payload:
                last_status = payload
                finished_at = int(payload.get("finished_at") or 0)
                status = str(payload.get("status") or "")
                if finished_at >= started_at - 2 and status:
                    if status not in {"local_reconciled", "not_configured"}:
                        _log("PBRun", f"PBCluster boot sync status: {status}", level="WARNING")
                    return {"status": status, "waited": max(0, int(time()) - started_at)}
        if timeout <= 0:
            break
        sleep(1)

    previous_status = str(last_status.get("status") or "missing") if last_status else "missing"
    _log(
        "PBRun",
        f"PBCluster boot sync did not complete within {max(0, int(timeout))}s; continuing with local desired state ({previous_status})",
        level="WARNING",
    )
    return {"status": "timeout", "previous_status": previous_status, "waited": max(0, int(time()) - started_at)}


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


def _configured_secret_value(value) -> bool:
    normalized = str(value or "").strip()
    if not normalized:
        return False
    lowered = normalized.lower()
    if lowered in {"none", "null", "false", "<api_key>"}:
        return False
    return not (normalized.startswith("<") and normalized.endswith(">"))

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
        self._dynamic_bootstrap_log_ts = 0
        self._dynamic_bootstrap_refresh_ts = 0
        self._dynamic_watch_ts = 0
        self._dynamic_watch_sig = None
        self._cluster_gate_log_ts = 0
        self._cluster_gate_log_key = None
        self.cluster_blocked = False
        self.cluster_blocked_reason = ""
        self.cluster_gate = "not_checked"
        self.start_time = 0
        self.memory = None
        self.cpu = None

    def _cluster_gate_result(self) -> dict:
        """Return whether Cluster Sync desired state allows this V7 bot to run."""

        pbgdir = Path(self.pbgdir or Path.cwd())
        cluster_root = default_cluster_root(pbgdir)
        if not _cluster_gate_is_configured(cluster_root):
            return {"ok": True, "status": "not_configured", "reason": "Cluster Sync is not initialized"}

        try:
            identity = read_local_identity(cluster_root)
        except Exception as exc:
            return {"ok": False, "status": "identity_error", "reason": f"Cluster identity is invalid: {exc}"}

        desired_path = cluster_root / "desired_state.json"
        if not desired_path.is_file():
            return {"ok": False, "status": "missing_desired_state", "reason": "Cluster desired_state.json is missing"}
        try:
            desired = _read_json_file(desired_path)
        except Exception as exc:
            return {"ok": False, "status": "desired_state_error", "reason": f"Cluster desired_state.json is unreadable: {exc}"}

        cluster_id = str(identity.get("cluster_id") or "")
        if str(desired.get("cluster_id") or "") != cluster_id:
            return {"ok": False, "status": "foreign_desired_state", "reason": "Cluster desired_state.json belongs to another cluster"}

        instance_name = str(self.user or Path(str(self.path or "")).name)
        tombstones = desired.get("tombstones") if isinstance(desired.get("tombstones"), dict) else {}
        if instance_name in tombstones:
            return {"ok": False, "status": "tombstoned", "reason": "Cluster desired state tombstoned this instance"}

        instances = desired.get("instances") if isinstance(desired.get("instances"), dict) else {}
        item = instances.get(instance_name)
        if not isinstance(item, dict):
            return {"ok": False, "status": "missing_instance", "reason": "Instance is missing from Cluster desired state"}
        if item.get("conflicted") is True:
            return {"ok": False, "status": "conflicted", "reason": "Cluster desired state marks this instance as conflicted"}
        if str(item.get("desired_state") or "") != "running":
            return {"ok": False, "status": "desired_stopped", "reason": "Cluster desired state is not running"}

        local_node_id = str(identity.get("node_id") or "")
        assigned_host = str(item.get("assigned_host") or "")
        if assigned_host != local_node_id:
            return {"ok": False, "status": "wrong_host", "reason": "Cluster desired state assigns this instance to another node"}

        expected_hash = str(item.get("config_manifest_hash") or "")
        try:
            actual_hash = compute_config_manifest_hash(build_config_manifest(Path(str(self.path))))
        except Exception as exc:
            return {"ok": False, "status": "manifest_error", "reason": f"Cluster config manifest check failed: {exc}"}
        if actual_hash != expected_hash:
            return {"ok": False, "status": "manifest_mismatch", "reason": "Local config manifest does not match Cluster desired state"}

        expected_version = str(item.get("version") or "")
        if str(self.version or "") != expected_version:
            return {"ok": False, "status": "version_mismatch", "reason": "Local config version does not match Cluster desired state"}

        return {"ok": True, "status": "allowed", "reason": "Cluster desired state allows start"}

    def _set_cluster_gate_state(self, result: dict) -> None:
        """Record the last Cluster Sync gate result on this runner."""

        self.cluster_gate = str(result.get("status") or "")
        self.cluster_blocked = not bool(result.get("ok"))
        self.cluster_blocked_reason = "" if result.get("ok") else str(result.get("reason") or "")

    def _block_cluster_gate_start(self, result: dict) -> None:
        """Stop or delay this bot because Cluster Sync desired state blocks it."""

        self._set_cluster_gate_state(result)
        _atomic_write_text(Path(self.path) / "running_version.txt", "0")
        now_ts = int(datetime.now().timestamp())
        log_key = (self.cluster_gate, self.cluster_blocked_reason)
        quiet_states = {"desired_stopped", "missing_instance", "tombstoned", "wrong_host"}
        quiet_state = self.cluster_gate in quiet_states
        should_log = log_key != self._cluster_gate_log_key or (not quiet_state and now_ts - self._cluster_gate_log_ts >= 60)
        if should_log:
            _log(
                "PBRun",
                f"Cluster gate blocked passivbot_v7 {self.path}/config_run.json: {self.cluster_blocked_reason}",
                level="INFO" if quiet_state else "WARNING",
            )
            self._cluster_gate_log_ts = now_ts
            self._cluster_gate_log_key = log_key

    def _cluster_gate_allows_run(self) -> bool:
        """Return True when this bot is allowed to run under Cluster Sync."""

        result = self._cluster_gate_result()
        self._set_cluster_gate_state(result)
        if result.get("ok"):
            return True
        self._block_cluster_gate_start(result)
        return False

    def _dynamic_watch_signature(self):
        if self.dynamic_ignore is None:
            return None

        pbgdir = Path.cwd()
        paths = [pbgdir / "data" / "coindata" / "metadata.json"]

        signature = []
        for path in paths:
            try:
                stat = path.stat()
                signature.append((str(path), stat.st_mtime_ns, stat.st_size))
            except FileNotFoundError:
                signature.append((str(path), None, None))
            except OSError:
                signature.append((str(path), None, None))
        return tuple(signature)

    def _bootstrap_dynamic_ignore_data(self) -> bool:
        try:
            if self.dynamic_ignore is None:
                return True

            exchange_id = getattr(self.dynamic_ignore.coindata, "exchange", None)
            if not exchange_id:
                return False

            self.dynamic_ignore.coindata.load_config()
            if not _configured_secret_value(getattr(self.dynamic_ignore.coindata, "api_key", None)):
                return False

            # First try to build or reuse list files from whatever local mapping data
            # is already available, avoiding a CMC/CCXT refresh unless it is needed.
            self.dynamic_ignore.watch()
            if self.dynamic_ignore.lists_ready():
                return True

            needs_refresh, reason = self.dynamic_ignore.coindata._source_is_newer_than_mapping(exchange_id)
            if not needs_refresh:
                return False

            now_ts = int(datetime.now().timestamp())
            refresh_interval_s = 300
            if now_ts - self._dynamic_bootstrap_refresh_ts < refresh_interval_s:
                return False

            if now_ts - self._dynamic_bootstrap_log_ts >= 60:
                _log(
                    "PBRun",
                    f"Bootstrap dynamic_ignore data for {self.path} ({exchange_id}): {reason}",
                    level="INFO",
                )
                self._dynamic_bootstrap_log_ts = now_ts

            self.dynamic_ignore.coindata.load_data()
            self.dynamic_ignore.coindata.load_metadata()
            result = self.dynamic_ignore.coindata.refresh_exchange_mapping(exchange_id)
            self._dynamic_bootstrap_refresh_ts = now_ts
            if not bool(result.get("ok")):
                _log(
                    "PBRun",
                    (
                        f"Dynamic_ignore bootstrap refresh failed for {self.path} ({exchange_id}): "
                        f"markets_ok={result.get('markets_ok')} mapping_ok={result.get('mapping_ok')} "
                        f"prices_ok={result.get('prices_ok')}"
                    ),
                    level="WARNING",
                )
                return False

            self.dynamic_ignore.watch()
            return self.dynamic_ignore.lists_ready()
        except Exception as e:
            _log("PBRun", f"Dynamic_ignore bootstrap error for {self.path}: {e}", level="ERROR")
            _log("PBRun", "Dynamic_ignore bootstrap traceback", level="DEBUG", meta={"traceback": traceback.format_exc()})
            return False

    def _dynamic_ignore_api_key_configured(self) -> bool:
        if self.dynamic_ignore is None:
            return True
        coindata = getattr(self.dynamic_ignore, "coindata", None)
        if coindata is None:
            return True
        try:
            coindata.load_config()
        except Exception:
            pass
        return _configured_secret_value(getattr(coindata, "api_key", None))

    def _delay_dynamic_ignore_start(self, reason: str):
        _atomic_write_text(Path(self.path) / "running_version.txt", "0")
        now_ts = int(datetime.now().timestamp())
        if now_ts - self._dynamic_wait_log_ts >= 60:
            _log(
                "PBRun",
                f"Delay start: passivbot_v7 {self.path}/config_run.json {reason}",
                level="WARNING",
            )
            self._dynamic_wait_log_ts = now_ts

    def watch(self):
        if self.is_running():
            if not self._cluster_gate_allows_run():
                self.stop()
                return
            version_file = Path(f'{self.path}/running_version.txt')
            current_version = 0
            if version_file.exists():
                try:
                    current_version = int(version_file.read_text().strip())
                except (ValueError, OSError):
                    current_version = 0
            if current_version != self.version:
                _log("PBRun", f"Repair running_version for {self.user}: {current_version} -> {self.version}")
                self.create_v7_running_version()
            return
        if not self.is_running():
            self.start()

    def watch_dynamic(self):
        if self.dynamic_ignore is None:
            return

        current_sig = self._dynamic_watch_signature()
        if current_sig == self._dynamic_watch_sig:
            return

        now_ts = int(datetime.now().timestamp())
        if self._dynamic_watch_ts and now_ts - self._dynamic_watch_ts < 60:
            return

        self.dynamic_ignore.watch()
        self._dynamic_watch_ts = now_ts
        self._dynamic_watch_sig = current_sig

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
                _attach_process_stats(process, self)
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
            if Path(f'{self.path}/config.json').exists() and not self.load():
                self.stop()
                return
            if not self._cluster_gate_allows_run():
                return
            if self.dynamic_ignore is not None and not _ensure_dynamic_ignore_ready(self.dynamic_ignore):
                if not self._dynamic_ignore_api_key_configured():
                    self._delay_dynamic_ignore_start("requires CoinMarketCap API key for dynamic_ignore")
                    return
                if not self._bootstrap_dynamic_ignore_data():
                    self._delay_dynamic_ignore_start("waiting for dynamic ignore lists")
                    return
            self._dynamic_wait_log_ts = 0
            old_os_path = os.environ.get('PATH', '')
            new_os_path = os.path.dirname(self.pb7venv) + os.pathsep + old_os_path
            os.environ['PATH'] = new_os_path
            try:
                cmd = [self.pb7venv, '-u', PurePath(f'{self.pb7dir}/src/main.py'), PurePath(f'{self.path}/config_run.json')]
                err_log = str(Path(f'{self.path}/passivbot_err.log'))
                if platform.system() == "Windows":
                    creationflags = subprocess.DETACHED_PROCESS
                    creationflags |= subprocess.CREATE_NO_WINDOW
                    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, cwd=self.pb7dir, text=True, creationflags=creationflags)
                else:
                    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, cwd=self.pb7dir, text=True, start_new_session=True)
                threading.Thread(target=_ts_wrap_stderr, args=(proc.stderr, err_log), daemon=True).start()
            finally:
                os.environ['PATH'] = old_os_path
            _log("PBRun", f"Start: passivbot_v7 {self.path}/config_run.json")
        # wait until passivbot is running
        for i in range(10):
            if self.is_running():
                self.create_v7_running_version()
                break
            sleep(1)

    def clean_log(self):
        err_log = Path(f'{self.path}/passivbot_err.log')
        max_bytes, backup_count = get_rotate_settings(logfile=err_log)
        rotate_logfile_if_oversize(str(err_log), max_bytes, backup_count)
        # delete old passivbot.log files (no longer used)
        for old in (Path(f'{self.path}/passivbot.log'), Path(f'{self.path}/passivbot.log.old')):
            try:
                old.unlink(missing_ok=True)
            except Exception:
                pass

    def create_v7_running_version(self):
        # Write running Version to file
        version_file = Path(f'{self.path}/running_version.txt')
        _atomic_write_text(version_file, str(self.version))

    def load(self):
        """Load version for PB v7."""
        file = Path(f'{self.path}/config.json')
        file_run = Path(f'{self.path}/config_run.json')
        if file.exists():
            try:
                with open(file, "r", encoding='utf-8') as f:
                    v7_config = f.read()
                self._v7_config = json.loads(v7_config)
                self.version = self._v7_config["pbgui"]["version"]
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
        self.run_v7 = []
        self.index = 0
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
        # Init PB7 directory
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
        # Init PB7 virtual environment
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

    @staticmethod
    def _git_dir(repo_dir) -> Path | None:
        """Return a repository .git directory when it exists."""
        if not repo_dir:
            return None
        git_dir = Path(repo_dir) / ".git"
        return git_dir if git_dir.exists() else None

    @staticmethod
    def _first_version_token(text: str, line_limit: int | None = 20) -> str:
        """Extract the first vN.N-style version token from text."""
        lines = str(text or "").splitlines()
        if line_limit is not None:
            lines = lines[: int(line_limit)]
        for line in lines:
            match = re.search(r"\bv\d+(?:\.\d+)+\b", line)
            if match:
                return match.group(0)
        return "N/A"

    @staticmethod
    def _git_text(repo_dir, args: list[str], timeout: int = 20) -> str:
        """Run a git command for a repo and return stdout or an empty string."""
        git_dir = PBRun._git_dir(repo_dir)
        if git_dir is None:
            return ""
        result = _run_subprocess(["git", "--git-dir", str(git_dir)] + list(args), timeout=timeout)
        if not result or getattr(result, "returncode", 1) != 0:
            return ""
        return str(getattr(result, "stdout", "") or "").strip()

    def get_current_pbgui_status(self) -> tuple[str, str]:
        """Return the current PBGui git branch and commit hash."""
        commit = self._git_text(self.pbgdir, ["rev-parse", "HEAD"])
        branch = self._git_text(self.pbgdir, ["symbolic-ref", "--short", "HEAD"]) or "unknown"
        return branch, commit

    def get_current_pb7_status(self) -> tuple[str, str]:
        """Return the current PB7 git branch and commit hash."""
        commit = self._git_text(self.pb7dir, ["rev-parse", "HEAD"])
        branch = self._git_text(self.pb7dir, ["symbolic-ref", "--short", "HEAD"]) or "unknown"
        return branch, commit

    def _load_branch_history(self, repo_dir, current_branch: str, limit: int = 50) -> dict[str, list[dict]]:
        """Load git branch history for one repository."""
        git_dir = self._git_dir(repo_dir)
        if git_dir is None:
            return {}
        _run_subprocess(["git", "--git-dir", str(git_dir), "fetch", "origin"], timeout=20)
        branches_result = _run_subprocess(["git", "--git-dir", str(git_dir), "branch", "-a"], timeout=15)
        if not branches_result or getattr(branches_result, "returncode", 1) != 0:
            return {}

        branch_lines = str(getattr(branches_result, "stdout", "") or "").splitlines()
        remote_branches: set[str] = set()
        for line in branch_lines:
            branch_raw = line.strip().lstrip("* ")
            if branch_raw.startswith("remotes/origin/") and "HEAD ->" not in branch_raw:
                remote_branches.add(branch_raw.replace("remotes/origin/", ""))

        branches_data: dict[str, dict] = {}
        for line in branch_lines:
            branch_raw = line.strip().lstrip("* ")
            if not branch_raw or "HEAD ->" in branch_raw:
                continue
            if branch_raw.startswith("remotes/origin/"):
                branch_ref = branch_raw
                branch_name = branch_raw.replace("remotes/origin/", "")
            else:
                branch_name = branch_raw
                if branch_name in remote_branches:
                    continue
                branch_ref = branch_raw
            if branch_name in branches_data:
                continue
            commits_result = _run_subprocess(
                [
                    "git", "--git-dir", str(git_dir), "log", branch_ref, "-n", str(limit),
                    "--pretty=format:%h|%H|%an|%ar|%at|%B%x00",
                ],
                timeout=20,
            )
            if not commits_result or getattr(commits_result, "returncode", 1) != 0:
                continue
            commits, latest_ts = _parse_git_log_output(str(getattr(commits_result, "stdout", "") or ""), branch_name)
            if commits:
                branches_data[branch_name] = {"commits": commits, "latest_timestamp": latest_ts}

        sorted_data = sorted(
            branches_data.items(),
            key=lambda item: item[1].get("latest_timestamp") or 0,
            reverse=True,
        )
        return {name: data["commits"] for name, data in sorted_data}

    def load_git_branches_history(self, limit: int = 50):
        """Load PBGui and PB7 branch history into instance attributes."""
        self.pbgui_branches_data = self._load_branch_history(self.pbgdir, getattr(self, "pbgui_branch", ""), limit=limit)
        self.pb7_branches_data = self._load_branch_history(self.pb7dir, getattr(self, "pb7_branch", ""), limit=limit)

    def load_more_commits(self, branch_name: str, limit: int = 50):
        """Load more PBGui commits for the requested branch."""
        git_dir = self._git_dir(self.pbgdir)
        if git_dir is None or not branch_name:
            return
        _run_subprocess(["git", "--git-dir", str(git_dir), "fetch", "origin"], timeout=20)
        current_branch = getattr(self, "pbgui_branch", "") or self.get_current_pbgui_status()[0]
        branch_ref = f"remotes/origin/{branch_name}" if branch_name != current_branch else branch_name
        commits_result = _run_subprocess(
            ["git", "--git-dir", str(git_dir), "log", branch_ref, "-n", str(limit), "--pretty=format:%h|%H|%an|%ar|%at|%B%x00"],
            timeout=20,
        )
        if not commits_result or getattr(commits_result, "returncode", 1) != 0:
            return
        commits, _ = _parse_git_log_output(str(getattr(commits_result, "stdout", "") or ""), branch_name)
        self.pbgui_branches_data[branch_name] = commits

    def load_git_commits(self):
        """Load current local commit hashes without overwriting defaults on failure."""
        pbgui_branch, pbgui_commit = self.get_current_pbgui_status()
        pb7_branch, pb7_commit = self.get_current_pb7_status()
        if pbgui_commit:
            self.pbgui_branch = pbgui_branch
            self.pbgui_commit = pbgui_commit
        if pb7_commit:
            self.pb7_branch = pb7_branch
            self.pb7_commit = pb7_commit

    def load_versions_origin(self):
        """Load origin README versions for PBGui and PB7."""
        pbgui_text = self._git_text(self.pbgdir, ["show", "origin/main:README.md"])
        if pbgui_text:
            version = self._first_version_token(pbgui_text, line_limit=None)
            if version != "N/A":
                self.pbgui_version_origin = version
        pb7_text = self._git_text(self.pb7dir, ["show", "origin/master:README.md"])
        if pb7_text:
            version = self._first_version_token(pb7_text, line_limit=None)
            if version != "N/A":
                self.pb7_version_origin = version

    def load_versions(self):
        """Load local README versions for PBGui and PB7 from their first 20 lines."""
        for attr, repo_dir in (("pbgui_version", self.pbgdir), ("pb7_version", self.pb7dir)):
            try:
                readme = Path(repo_dir) / "README.md"
                if not readme.exists():
                    continue
                version = self._first_version_token(readme.read_text(encoding="utf-8", errors="ignore"), line_limit=20)
                if version != "N/A":
                    setattr(self, attr, version)
            except Exception:
                continue

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
                        copytree(source, destination, dirs_exist_ok=True, ignore=ignore_patterns('passivbot.log', 'passivbot.log.old', 'ignored_coins.json', 'approved_coins.json', 'config_run.json'))
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
                    copytree(source, destination, dirs_exist_ok=True, ignore=ignore_patterns('passivbot.log', 'passivbot.log.old', 'ignored_coins.json', 'approved_coins.json', 'config_run.json'))
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
                        if not run_v7._cluster_gate_allows_run():
                            run_v7.stop()
                        else:
                            running_version = self.find_running_version(v7_instance)
                            if running_version < run_v7.version:
                                run_v7.stop()
                                run_v7.start()
                    else:
                        run_v7.start()
                    self.add_v7(run_v7)
                    status.running = run_v7.is_running()
                else:
                    self.remove_v7(run_v7)
                    status.running = False
                    run_v7.stop()
                status.version = run_v7.version
                status.enabled_on = run_v7.name
                # Restore per-instance activate_ts
                status.activate_ts = old_ts.get(status.name, 0)
                status.blocked = bool(getattr(run_v7, "cluster_blocked", False))
                status.blocked_reason = str(getattr(run_v7, "cluster_blocked_reason", "") or "")
                status.cluster_gate = str(getattr(run_v7, "cluster_gate", "") or "")
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
    _wait_for_cluster_boot_sync(Path(run.pbgdir), timeout=20)
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
