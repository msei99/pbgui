"""Manage local Passivbot V7 and V8 live processes."""
import psutil
import signal
import stat
import subprocess
import threading
import sys
from pathlib import Path, PurePath
from time import sleep, time
import json
import re
from datetime import datetime
import platform
import os
import traceback
from PBCoinData import CoinData
import pbgui_purefunc
from logging_helpers import human_log as _log, get_rotate_settings, rotate_logfile_if_oversize
from master_update_lock import MasterUpdateBusyError, acquire_master_runtime_lock

SERVICE = "PBRun"
from master.cluster_state import (
    SYNC_EXCLUDE_FILES,
    build_config_manifest,
    compute_config_manifest_hash,
    default_cluster_root,
    read_local_identity,
)


V7_RUNTIME_SIGNATURE_EXCLUDE_FILES = frozenset({
    "approved_coins.json",
    "config_run.json",
    "ignored_coins.json",
    "running_version.txt",
})

CLUSTER_PRE_LOAD_BLOCK_STATES = frozenset({
    "desired_stopped",
    "missing_instance",
    "tombstoned",
    "wrong_host",
})

CLUSTER_QUIET_BLOCK_STATES = CLUSTER_PRE_LOAD_BLOCK_STATES

PB8_STABLE_SECONDS = 60
PB8_BACKOFF_INITIAL_SECONDS = 5
PB8_BACKOFF_MAX_SECONDS = 300
PB8_RUST_PROBE_CODE = """
import json
import sys
sys.path.insert(0, "src")
import rust_utils
rust_utils.prune_shadowing_local_extensions()
path = rust_utils.preferred_compiled_path()
source_mtime = rust_utils.latest_source_mtime()
fingerprint = rust_utils.source_fingerprint()
stamp = rust_utils.read_source_stamp(path) if path is not None else None
print(json.dumps({
    "path": str(path) if path is not None else "",
    "stamped": bool(stamp),
    "needs_rebuild": rust_utils.extension_needs_rebuild(path, source_mtime, fingerprint),
}))
"""


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
                        _log(SERVICE, f"PBCluster boot sync status: {status}", level="WARNING")
                    return {"status": status, "waited": max(0, int(time()) - started_at)}
        if timeout <= 0:
            break
        sleep(1)

    previous_status = str(last_status.get("status") or "missing") if last_status else "missing"
    _log(SERVICE,
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
        _log(SERVICE, f"Timed out waiting for process to stop ({context})", level="WARNING")
    except psutil.AccessDenied as e:
        _log(SERVICE, f"Access denied while stopping process ({context}): {e}", level="ERROR")


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
        _log(SERVICE, f"Command timeout after {timeout}s: {' '.join(map(str, command))}", level="WARNING")
        return None
    except FileNotFoundError as e:
        _log(SERVICE, f"Command not found: {' '.join(map(str, command))} ({e})", level="WARNING")
        return None
    except Exception as e:
        _log(SERVICE, f"Command failed: {' '.join(map(str, command))} ({e})", level="ERROR")
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
            _log(SERVICE,
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
                _log(SERVICE, f"Change ignored_coins {self.path} Removed: {removed_coins} Added: {added_coins}")
                self.ignored_coins = ignored_coins

            if approved_changed:
                removed_coins = sorted(set(self.approved_coins) - set(approved_coins))
                added_coins = sorted(set(approved_coins) - set(self.approved_coins))
                _log(SERVICE, f"Change approved_coins {self.path} Removed: {removed_coins} Added: {added_coins}")
                self.approved_coins = approved_coins

            if ignored_changed or approved_changed:
                self.save()
                return True
            return False
        except Exception as e:
            _log(SERVICE, f"DynamicIgnore watch error for {self.path}: {e}", level="ERROR")
            _log(SERVICE, "DynamicIgnore watch traceback", level="DEBUG", meta={"traceback": traceback.format_exc()})
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
                _log(SERVICE, f"Change approved_coins {self.path} Removed: {symbol} because it is in ignored_coins")
                approved_coins.remove(symbol)

        ignored_set = set(ignored_coins)
        for symbol in self._normalize_symbol_list(self.approved_coins_long + self.approved_coins_short):
            if symbol in ignored_set:
                if symbol in approved_coins:
                    _log(SERVICE, f"Change approved_coins {self.path} Removed: {symbol} because it is in ignored_coins")
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

    def _block_cluster_gate_start(self, result: dict, *, log: bool = True) -> None:
        """Stop or delay this bot because Cluster Sync desired state blocks it."""

        self._set_cluster_gate_state(result)
        _atomic_write_text(Path(self.path) / "running_version.txt", "0")
        if not log or self.cluster_gate in CLUSTER_QUIET_BLOCK_STATES:
            return
        now_ts = int(datetime.now().timestamp())
        log_key = (self.cluster_gate, self.cluster_blocked_reason)
        should_log = log_key != self._cluster_gate_log_key or now_ts - self._cluster_gate_log_ts >= 60
        if should_log:
            _log(SERVICE,
                f"Cluster gate blocked passivbot_v7 {self.path}/config_run.json: {self.cluster_blocked_reason}",
                level="WARNING",
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
            if not self._dynamic_ignore_api_key_configured():
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
                _log(SERVICE,
                    f"Bootstrap dynamic_ignore data for {self.path} ({exchange_id}): {reason}",
                    level="INFO",
                )
                self._dynamic_bootstrap_log_ts = now_ts

            self.dynamic_ignore.coindata.load_data()
            self.dynamic_ignore.coindata.load_metadata()
            result = self.dynamic_ignore.coindata.refresh_exchange_mapping(exchange_id)
            self._dynamic_bootstrap_refresh_ts = now_ts
            if not bool(result.get("ok")):
                _log(SERVICE,
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
            _log(SERVICE, f"Dynamic_ignore bootstrap error for {self.path}: {e}", level="ERROR")
            _log(SERVICE, "Dynamic_ignore bootstrap traceback", level="DEBUG", meta={"traceback": traceback.format_exc()})
            return False

    def _dynamic_ignore_api_key_configured(self) -> bool:
        if self.dynamic_ignore is None:
            return True
        coindata = getattr(self.dynamic_ignore, "coindata", None)
        if coindata is None:
            return True
        try:
            ready = getattr(coindata, "cmc_pool_ready", False)
            return bool(ready() if callable(ready) else ready)
        except Exception:
            return False

    def _delay_dynamic_ignore_start(self, reason: str):
        _atomic_write_text(Path(self.path) / "running_version.txt", "0")
        now_ts = int(datetime.now().timestamp())
        if now_ts - self._dynamic_wait_log_ts >= 60:
            _log(SERVICE,
                f"Delay start: passivbot_v7 {self.path}/config_run.json {reason}",
                level="WARNING",
            )
            self._dynamic_wait_log_ts = now_ts

    def watch(self):
        if self.cluster_blocked and self.cluster_gate in CLUSTER_QUIET_BLOCK_STATES:
            return
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
                _log(SERVICE, f"Repair running_version for {self.user}: {current_version} -> {self.version}")
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
            _log(SERVICE,
                f"Stop: passivbot v7 {self.path}/config_run.json",
                user=self.user,
                meta={"operation": "stop_passivbot_v7", "instance": self.user},
            )
            _kill_process(process, f"v7 {self.path}")
        # Always write 0 — even if bot already crashed and no process found.
        # This ensures running_version.txt reflects "stopped" for inotify → UI.
        version_file = Path(f'{self.path}/running_version.txt')
        _atomic_write_text(version_file, "0")

    def start(self, *, reload_config: bool = True):
        if not self.is_running():
            pre_load_gate = self._cluster_gate_result()
            if not pre_load_gate.get("ok") and str(pre_load_gate.get("status") or "") in CLUSTER_PRE_LOAD_BLOCK_STATES:
                self._block_cluster_gate_start(pre_load_gate)
                return
            if reload_config and Path(f'{self.path}/config.json').exists() and not self.load():
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
            _log(SERVICE,
                f"Start: passivbot_v7 {self.path}/config_run.json",
                user=self.user,
                meta={"operation": "start_passivbot_v7", "instance": self.user},
            )
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
                _log(SERVICE, f"Something went wrong, but continue {e}", level="ERROR")
                _log(SERVICE, f"Setting version of {self.user} to 0", level="WARNING")
                self.version = 0
                _log(SERVICE, "RunV7.load traceback", level="DEBUG", meta={"traceback": traceback.format_exc()})


class RunV8:
    """Validate and supervise one materialized PB8 live configuration."""

    def __init__(self):
        self.user = None
        self.path = None
        self.name = None
        self.version = None
        self.pb8dir = None
        self.pb8venv = None
        self.pbgdir = None
        self.live_user = None
        self.start_time = 0
        self.memory = None
        self.cpu = None
        self.cluster_blocked = False
        self.cluster_blocked_reason = ""
        self.cluster_gate = "not_checked"
        self._last_started_at = 0.0
        self._next_start_at = 0.0
        self._crash_count = 0
        self._running_version = None
        self._block_log_key = None
        self._block_log_ts = 0.0

    @property
    def config_path(self) -> Path:
        """Return the canonical absolute config path used in process identity."""

        return (Path(self.path) / "config.json").resolve()

    @property
    def venv_dir(self) -> Path:
        """Return the configured PB8 virtual environment root."""

        return Path(self.pb8venv).expanduser().absolute().parent.parent

    @property
    def command(self) -> list[str]:
        """Return the exact PB8 live command for this instance."""

        return [
            str(self.venv_dir / "bin" / "passivbot"),
            "live",
            str(self.config_path),
            "--fail-on-stale-rust",
        ]

    def _log_block(self, key: str, reason: str) -> None:
        """Rate-limit non-secret start-gate diagnostics."""

        now = time()
        if self._block_log_key != key or now - self._block_log_ts >= 60:
            _log(SERVICE, f"PB8 start blocked for {self.user}: {reason}", level="WARNING")
            self._block_log_key = key
            self._block_log_ts = now

    def load(self) -> bool:
        """Load and validate PBGui metadata plus PB8 API-key user presence."""

        try:
            payload = _read_json_file(self.config_path)
            pbgui = payload.get("pbgui") if isinstance(payload.get("pbgui"), dict) else {}
            live = payload.get("live") if isinstance(payload.get("live"), dict) else {}
            raw_version = pbgui.get("version")
            if str(pbgui.get("runtime") or "").strip().lower() != "pb8":
                raise ValueError("pbgui.runtime must be pb8")
            if str(pbgui.get("enabled_on") or "").strip() != str(self.name or "").strip():
                raise ValueError("pbgui.enabled_on is not assigned to this host")
            if isinstance(raw_version, bool) or not str(raw_version or "").isdigit() or int(raw_version) < 1:
                raise ValueError("pbgui.version must be a positive integer")
            live_user = str(live.get("user") or "").strip()
            if not live_user:
                raise ValueError("live.user is required")

            api_keys_path = Path(self.pb8dir) / "api-keys.json"
            api_stat = os.stat(api_keys_path, follow_symlinks=False)
            if not stat.S_ISREG(api_stat.st_mode):
                raise ValueError("PB8 api-keys.json must be a regular file")
            api_keys = _read_json_file(api_keys_path)
            if live_user not in api_keys:
                raise ValueError("live.user is absent from PB8 api-keys.json")

            self.version = int(raw_version)
            self.live_user = live_user
            return True
        except Exception as exc:
            self.version = None
            self.live_user = None
            self._log_block("invalid_config", str(exc))
            return False

    @staticmethod
    def _pb8_desired_instances(desired: dict) -> tuple[dict | None, dict]:
        """Return an optional PB8 desired-state map and its tombstones."""

        for key, tombstone_key in (("pb8_instances", "pb8_tombstones"), ("instances_v8", "tombstones_v8")):
            value = desired.get(key)
            if key in desired or tombstone_key in desired:
                tombstones = desired.get(tombstone_key)
                return value if isinstance(value, dict) else {}, tombstones if isinstance(tombstones, dict) else {}
        pb8_state = desired.get("pb8")
        if isinstance(pb8_state, dict) and isinstance(pb8_state.get("instances"), dict):
            tombstones = pb8_state.get("tombstones")
            return pb8_state["instances"], tombstones if isinstance(tombstones, dict) else {}
        shared = desired.get("instances")
        if isinstance(shared, dict):
            pb8_items = {
                str(name): item
                for name, item in shared.items()
                if isinstance(item, dict) and str(item.get("runtime") or "").strip().lower() == "pb8"
            }
            if pb8_items:
                tombstones = desired.get("tombstones")
                return pb8_items, tombstones if isinstance(tombstones, dict) else {}
        return None, {}

    def _cluster_gate_result(self) -> dict:
        """Apply V7-equivalent checks only when PB8 desired state is present."""

        cluster_root = default_cluster_root(Path(self.pbgdir or Path.cwd()))
        desired_path = cluster_root / "desired_state.json"
        if not _cluster_gate_is_configured(cluster_root) or not desired_path.is_file():
            return {"ok": True, "status": "not_configured", "reason": "PB8 desired state is not configured"}
        try:
            desired = _read_json_file(desired_path)
            instances, tombstones = self._pb8_desired_instances(desired)
            if instances is None:
                return {"ok": True, "status": "not_configured", "reason": "PB8 desired state is not configured"}
            identity = read_local_identity(cluster_root)
        except Exception as exc:
            return {"ok": False, "status": "desired_state_error", "reason": f"PB8 desired state is invalid: {exc}"}

        if str(desired.get("cluster_id") or "") != str(identity.get("cluster_id") or ""):
            return {"ok": False, "status": "foreign_desired_state", "reason": "PB8 desired state belongs to another cluster"}
        instance_name = str(self.user or Path(str(self.path or "")).name)
        if instance_name in tombstones:
            return {"ok": False, "status": "tombstoned", "reason": "PB8 desired state tombstoned this instance"}
        item = instances.get(instance_name)
        if not isinstance(item, dict):
            return {"ok": False, "status": "missing_instance", "reason": "Instance is missing from PB8 desired state"}
        if item.get("conflicted") is True:
            return {"ok": False, "status": "conflicted", "reason": "PB8 desired state marks this instance as conflicted"}
        if str(item.get("desired_state") or "") != "running":
            return {"ok": False, "status": "desired_stopped", "reason": "PB8 desired state is not running"}
        if str(item.get("assigned_host") or "") != str(identity.get("node_id") or ""):
            return {"ok": False, "status": "wrong_host", "reason": "PB8 desired state assigns this instance to another node"}
        if str(item.get("version") or "") != str(self.version or ""):
            return {"ok": False, "status": "version_mismatch", "reason": "Local PB8 config version does not match desired state"}
        expected_hash = str(item.get("config_manifest_hash") or "")
        try:
            actual_hash = compute_config_manifest_hash(build_config_manifest(Path(self.path)))
        except Exception as exc:
            return {"ok": False, "status": "manifest_error", "reason": f"PB8 config manifest check failed: {exc}"}
        if actual_hash != expected_hash:
            return {"ok": False, "status": "manifest_mismatch", "reason": "Local PB8 config does not match desired state"}
        return {"ok": True, "status": "allowed", "reason": "PB8 desired state allows start"}

    def _runtime_ready(self) -> bool:
        """Require the validated PB8 runtime and reject an invalid update marker."""

        invalid_marker = Path(self.pbgdir) / "data" / "locks" / "pb8-runtime-invalid"
        if invalid_marker.exists() or invalid_marker.is_symlink():
            self._log_block("runtime_invalid", "PB8 runtime is marked invalid")
            return False
        try:
            status_payload = pbgui_purefunc.pb8_runtime_status()
        except Exception:
            status_payload = {}
        if not status_payload.get("ready"):
            errors = status_payload.get("errors") if isinstance(status_payload.get("errors"), list) else []
            reason = str(errors[0]) if errors else "PB8 runtime is not ready"
            self._log_block("runtime_not_ready", reason)
            return False
        status_dir = str(status_payload.get("pb8dir") or "").strip()
        status_venv = str(status_payload.get("pb8venv") or "").strip()
        if status_dir and Path(status_dir).resolve() != Path(self.pb8dir).resolve():
            self._log_block("runtime_changed", "PB8 runtime configuration changed")
            return False
        if status_venv and Path(status_venv).resolve() != Path(self.pb8venv).resolve():
            self._log_block("runtime_changed", "PB8 runtime configuration changed")
            return False
        try:
            result = subprocess.run(
                [str(Path(self.pb8venv).expanduser().absolute()), "-c", PB8_RUST_PROBE_CODE],
                cwd=str(Path(self.pb8dir).resolve()),
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            if result.returncode != 0:
                detail = (result.stderr or result.stdout or "").strip()[-300:]
                raise RuntimeError(detail or "PB8 Rust probe failed")
            lines = [line for line in result.stdout.splitlines() if line.strip()]
            rust_status = json.loads(lines[-1]) if lines else {}
            if not isinstance(rust_status, dict):
                raise RuntimeError("PB8 Rust probe returned an invalid result")
        except (OSError, subprocess.SubprocessError, RuntimeError, json.JSONDecodeError) as exc:
            self._log_block("rust_probe_failed", f"PB8 Rust extension check failed: {exc}")
            return False
        if rust_status.get("needs_rebuild"):
            if not rust_status.get("stamped"):
                reason = "PB8 Rust extension has no source fingerprint stamp; rerun Update PB8 on this host"
            else:
                reason = "PB8 Rust extension is stale; rerun Update PB8 on this host"
            self._log_block("rust_stale", reason)
            return False
        return True

    def _matches_process(self, process: psutil.Process) -> bool:
        """Match only the exact command and working directory launched by RunV8."""

        try:
            cmdline = [str(arg) for arg in process.cmdline()]
            expected = self.command
            configured_python = str(Path(self.pb8venv).expanduser().absolute())
            resolved_python = str(Path(configured_python).resolve())
            command_matches = cmdline in (
                expected,
                [configured_python, *expected],
                [resolved_python, *expected],
            )
            if not command_matches:
                return False
            cwd_method = getattr(process, "cwd", None)
            if not callable(cwd_method) or Path(cwd_method()).resolve() != Path(self.pb8dir).resolve():
                return False
            return True
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied, OSError):
            return False

    @classmethod
    def from_missing_config_process(
        cls,
        process: psutil.Process,
        *,
        run_root: Path,
        pb8dir: str,
        pb8venv: str,
        pbgdir: Path,
        name: str,
    ) -> "RunV8 | None":
        """Reconstruct an exact managed runner whose config disappeared."""

        try:
            cmdline = [str(arg) for arg in process.cmdline()]
            python = str(Path(pb8venv).expanduser().absolute())
            resolved_python = str(Path(python).resolve())
            passivbot = str(Path(python).parent / "passivbot")
            if len(cmdline) == 4 and cmdline[:2] == [passivbot, "live"]:
                config_arg = cmdline[2]
            elif len(cmdline) == 5 and cmdline[:3] in (
                [python, passivbot, "live"],
                [resolved_python, passivbot, "live"],
            ):
                config_arg = cmdline[3]
            else:
                return None
            if cmdline[-1] != "--fail-on-stale-rust":
                return None

            root = run_root.resolve()
            config_path = Path(config_arg)
            if not config_path.is_absolute():
                return None
            config_path = config_path.resolve()
            relative = config_path.relative_to(root)
            if len(relative.parts) != 2 or relative.parts[1] != "config.json" or config_path.is_file():
                return None

            runner = cls()
            runner.path = str(config_path.parent)
            runner.user = relative.parts[0]
            runner.name = name
            runner.pb8dir = pb8dir
            runner.pb8venv = pb8venv
            runner.pbgdir = pbgdir
            return runner if runner._matches_process(process) else None
        except (ValueError, OSError, psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            return None

    def pid(self):
        """Return the exact PB8 process for this config, with current stats."""

        for process in psutil.process_iter():
            if not self._matches_process(process):
                continue
            self.start_time = process.create_time()
            try:
                self.memory = process.memory_full_info()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                self.memory = None
            try:
                self.cpu = process.cpu_percent()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                self.cpu = None
            return process
        return None

    def is_running(self) -> bool:
        """Return whether the exact PB8 process identity is running."""

        return self.pid() is not None

    @staticmethod
    def _wait_stopped(process: psutil.Process, timeout: int) -> bool:
        """Wait for a process and normalize psutil's gone/timeout outcomes."""

        try:
            process.wait(timeout=timeout)
            return True
        except psutil.NoSuchProcess:
            return True
        except (psutil.TimeoutExpired, subprocess.TimeoutExpired):
            return False

    def _same_process(self, process: psutil.Process, create_time: float) -> bool:
        """Revalidate the complete PB8 process identity before a destructive action."""

        try:
            return process.create_time() == create_time and self._matches_process(process)
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied, OSError):
            return False

    def _signal_group(self, process: psutil.Process, sig: signal.Signals, create_time: float) -> bool:
        """Revalidate and signal only the original dedicated PB8 process group."""

        if platform.system() != "Windows":
            try:
                if not self._same_process(process, create_time):
                    return False
                group_id = os.getpgid(process.pid)
                if group_id == process.pid and self._same_process(process, create_time):
                    os.killpg(group_id, sig)
                    return True
            except (ProcessLookupError, PermissionError, psutil.NoSuchProcess):
                return False
        if not self._same_process(process, create_time):
            return False
        process.send_signal(sig)
        return True

    def stop(self, process: psutil.Process | None = None) -> None:
        """Gracefully stop PB8 with bounded SIGINT, TERM, then KILL escalation."""

        process = process or self.pid()
        self._last_started_at = 0.0
        self._running_version = None
        if not process:
            return
        try:
            create_time = process.create_time()
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied, OSError):
            return
        if not self._same_process(process, create_time):
            return
        _log(
            SERVICE,
            f"Stop: passivbot v8 {self.config_path}",
            user=self.user,
            meta={"operation": "stop_passivbot_v8", "instance": self.user},
        )
        for sig, timeout in ((signal.SIGINT, 10), (signal.SIGTERM, 5), (signal.SIGKILL, 3)):
            try:
                if not self._signal_group(process, sig, create_time):
                    return
            except (psutil.NoSuchProcess, ProcessLookupError):
                return
            if self._wait_stopped(process, timeout):
                return
        _log(SERVICE, f"Timed out stopping passivbot v8 {self.user}", level="WARNING")

    def _record_crash(self) -> None:
        """Apply a bounded exponential delay after an early PB8 exit."""

        self._crash_count = min(self._crash_count + 1, 16)
        exponent = min(self._crash_count - 1, 10)
        delay = min(PB8_BACKOFF_INITIAL_SECONDS * (2 ** exponent), PB8_BACKOFF_MAX_SECONDS)
        self._next_start_at = time() + delay
        self._last_started_at = 0.0
        _log(SERVICE, f"PB8 {self.user} exited early; retrying in {delay}s", level="WARNING")

    def start(self, *, reload_config: bool = True) -> bool:
        """Start one validated PB8 live process in an isolated virtualenv."""

        if self.is_running() or time() < self._next_start_at:
            return False
        if reload_config and not self.load():
            return False
        gate = self._cluster_gate_result()
        self.cluster_gate = str(gate.get("status") or "")
        self.cluster_blocked = not bool(gate.get("ok"))
        self.cluster_blocked_reason = "" if gate.get("ok") else str(gate.get("reason") or "")
        if self.cluster_blocked:
            self._log_block(f"cluster_{self.cluster_gate}", self.cluster_blocked_reason)
            return False

        env = os.environ.copy()
        env["VIRTUAL_ENV"] = str(self.venv_dir)
        env["PATH"] = str(self.venv_dir / "bin") + os.pathsep + os.defpath
        err_log = str(Path(self.path) / "passivbot_err.log")
        runtime_lease = None
        try:
            runtime_lease = acquire_master_runtime_lock(Path(self.pbgdir))
            if self.is_running() or not self._runtime_ready():
                return False
            proc = subprocess.Popen(
                self.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=str(Path(self.pb8dir).resolve()),
                text=True,
                env=env,
                start_new_session=platform.system() != "Windows",
                creationflags=(subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW) if platform.system() == "Windows" else 0,
            )
        except MasterUpdateBusyError:
            self._log_block("runtime_update_busy", "PB8 is being installed or updated")
            return False
        except (OSError, subprocess.SubprocessError) as exc:
            _log(SERVICE, f"PB8 {self.user} failed to launch: {exc}", level="ERROR")
            self._record_crash()
            return False
        finally:
            if runtime_lease is not None:
                runtime_lease.release()
        threading.Thread(target=_ts_wrap_stderr, args=(proc.stdout, err_log), daemon=True).start()
        self._last_started_at = time()
        self._running_version = self.version
        _log(
            SERVICE,
            f"Start: passivbot_v8 {self.config_path}",
            user=self.user,
            meta={"operation": "start_passivbot_v8", "instance": self.user},
        )
        for _attempt in range(5):
            if self.is_running():
                return True
            sleep(0.2)
        self._record_crash()
        return False

    def watch(self) -> None:
        """Reconcile validation, desired state, crashes, and the live process."""

        process = self.pid()
        if not self.load():
            self.stop(process)
            return
        gate = self._cluster_gate_result()
        self.cluster_gate = str(gate.get("status") or "")
        self.cluster_blocked = not bool(gate.get("ok"))
        self.cluster_blocked_reason = "" if gate.get("ok") else str(gate.get("reason") or "")
        if self.cluster_blocked:
            self.stop(process)
            self._log_block(f"cluster_{self.cluster_gate}", self.cluster_blocked_reason)
            return

        now = time()
        if process:
            config_changed = False
            try:
                config_changed = self.config_path.stat().st_mtime > float(process.create_time())
            except (OSError, psutil.Error):
                pass
            if config_changed or (self._running_version is not None and self._running_version != self.version):
                self.stop()
                self.start(reload_config=False)
                return
            if self._running_version is None:
                self._running_version = self.version
            if self._last_started_at and now - self._last_started_at >= PB8_STABLE_SECONDS:
                self._crash_count = 0
                self._next_start_at = 0.0
                self._last_started_at = 0.0
            return
        if self._last_started_at:
            if now - self._last_started_at < PB8_STABLE_SECONDS:
                self._record_crash()
            else:
                self._crash_count = 0
                self._next_start_at = 0.0
                self._last_started_at = 0.0
        self.start(reload_config=False)

class PBRun():
    """PBRun manages local V7 and V8 passivbot instances.

    It reconciles local data/run_v7 configs against Cluster Sync desired state.

    Robustness notes:
    - Runtime state files (pid/version/monitor) are written atomically to reduce partial-write corruption.
    """
    def __init__(self):
        # self.run_instances = []
        self.coindata = CoinData()
        self.run_v7 = []
        self.run_v8 = []
        self.index = 0
        self.pbgdir = Path.cwd()
        ini_snapshot = pbgui_purefunc.load_ini_snapshot()
        # Init pbname
        if ini_snapshot.has_option("main", "pbname"):
            self.name = ini_snapshot.get("main", "pbname")
        else:
            self.name = platform.node()
        self._v7_runtime_signature = None
        self._v8_runtime_signature = None
        # Init PB7 directory
        self.pb7dir = None
        if ini_snapshot.has_option("main", "pb7dir"):
            self.pb7dir = ini_snapshot.get("main", "pb7dir")
        # Init PB7 virtual environment
        self.pb7venv = None
        if ini_snapshot.has_option("main", "pb7venv"):
            self.pb7venv = ini_snapshot.get("main", "pb7venv")
        self.pb7_ready = bool(self.pb7dir and self.pb7venv)
        self.pb8dir = ini_snapshot.get("main", "pb8dir") if ini_snapshot.has_option("main", "pb8dir") else None
        self.pb8venv = ini_snapshot.get("main", "pb8venv") if ini_snapshot.has_option("main", "pb8venv") else None
        self.pb8_ready = bool(self.pb8dir and self.pb8venv)
        # Init paths
        self.v7_path = f'{self.pbgdir}/data/run_v7'
        self.v8_path = f'{self.pbgdir}/data/run_v8'
        # Init pid
        self.piddir = Path(f'{self.pbgdir}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbrun.pid')
        self.my_pid = None

    @staticmethod
    def _snapshot_main_value(ini_snapshot, option: str) -> str | None:
        """Return one normalized optional main-section setting."""

        if not ini_snapshot.has_option("main", option):
            return None
        value = str(ini_snapshot.get("main", option) or "").strip()
        return value or None

    def refresh_runtime_config(self) -> tuple[bool, bool]:
        """Refresh runtime profiles and stop runners removed by an INI change."""

        ini_snapshot = pbgui_purefunc.load_ini_snapshot()
        new_name = self._snapshot_main_value(ini_snapshot, "pbname") or platform.node()
        new_pb7dir = self._snapshot_main_value(ini_snapshot, "pb7dir")
        new_pb7venv = self._snapshot_main_value(ini_snapshot, "pb7venv")
        new_pb8dir = self._snapshot_main_value(ini_snapshot, "pb8dir")
        new_pb8venv = self._snapshot_main_value(ini_snapshot, "pb8venv")

        name_changed = new_name != self.name
        v7_changed = name_changed or (new_pb7dir, new_pb7venv) != (self.pb7dir, self.pb7venv)
        v8_changed = name_changed or (new_pb8dir, new_pb8venv) != (self.pb8dir, self.pb8venv)

        if v7_changed:
            for runner in list(self.run_v7):
                runner.stop()
            self.run_v7 = []
            self._v7_runtime_signature = None
        if v8_changed:
            for runner in list(self.run_v8):
                runner.stop()
            self.run_v8 = []
            self._v8_runtime_signature = None

        self.name = new_name
        self.pb7dir = new_pb7dir
        self.pb7venv = new_pb7venv
        self.pb7_ready = bool(new_pb7dir and new_pb7venv)
        self.pb8dir = new_pb8dir
        self.pb8venv = new_pb8venv
        self.pb8_ready = bool(new_pb8dir and new_pb8venv)
        return v7_changed, v8_changed

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

    @staticmethod
    def _file_signature(path: Path) -> tuple[str, int | None, int | None]:
        """Return a cheap change signature for one file path."""

        try:
            stat = path.stat()
            return (str(path), int(stat.st_mtime_ns), int(stat.st_size))
        except OSError:
            return (str(path), None, None)

    @staticmethod
    def _desired_state_signature(path: Path) -> tuple:
        """Return a semantic desired-state signature, ignoring rebuild timestamps."""

        try:
            desired = _read_json_file(path)
        except OSError:
            return (str(path), None, None)
        except Exception as exc:
            return (str(path), "error", str(exc))

        instances = desired.get("instances") if isinstance(desired.get("instances"), dict) else {}
        instance_sig = []
        for name, item in sorted(instances.items()):
            if not isinstance(item, dict):
                continue
            instance_sig.append((
                str(name),
                str(item.get("assigned_host") or ""),
                str(item.get("config_manifest_hash") or ""),
                bool(item.get("conflicted") is True),
                str(item.get("desired_state") or ""),
                str(item.get("version") or ""),
            ))

        tombstones = desired.get("tombstones") if isinstance(desired.get("tombstones"), dict) else {}
        tombstone_sig = tuple(sorted(str(name) for name in tombstones.keys()))
        return (
            str(path),
            str(desired.get("cluster_id") or ""),
            tuple(instance_sig),
            tombstone_sig,
        )

    def _current_v7_runtime_signature(self) -> tuple:
        """Return the local Cluster/run_v7 signature PBRun reconciles against."""

        cluster_root = default_cluster_root(Path(self.pbgdir))
        signature: list[tuple] = [
            self._file_signature(cluster_root / "cluster_id"),
            self._file_signature(cluster_root / "node_id"),
            self._desired_state_signature(cluster_root / "desired_state.json"),
        ]
        run_root = Path(self.v7_path)
        if not run_root.is_dir():
            signature.append((str(run_root), None, None))
            return tuple(signature)

        for instance_dir in sorted(run_root.iterdir(), key=lambda item: item.name):
            if not instance_dir.is_dir():
                continue
            signature.append(("instance", instance_dir.name))
            for item in sorted(instance_dir.glob("*.json"), key=lambda path: path.name):
                if item.name in V7_RUNTIME_SIGNATURE_EXCLUDE_FILES:
                    continue
                file_sig = self._file_signature(item)
                signature.append((instance_dir.name, item.name, file_sig[1], file_sig[2]))
        return tuple(signature)

    def has_v7_runtime_changed(self) -> bool:
        """Poll Cluster desired state and local run_v7 configs for PBRun rescans."""

        signature = self._current_v7_runtime_signature()
        if self._v7_runtime_signature is None:
            self._v7_runtime_signature = signature
            return False
        if signature == self._v7_runtime_signature:
            return False
        self._v7_runtime_signature = signature
        _log(SERVICE, "Cluster/run_v7 state changed — rescanning v7 instances")
        self.watch_v7()
        return True

    def _current_v8_runtime_signature(self) -> tuple:
        """Return the local PB8 desired-state and config signature."""

        cluster_root = default_cluster_root(Path(self.pbgdir))
        desired_path = cluster_root / "desired_state.json"
        try:
            desired = _read_json_file(desired_path)
            instances = desired.get("pb8_instances") if isinstance(desired.get("pb8_instances"), dict) else {}
            tombstones = desired.get("pb8_tombstones") if isinstance(desired.get("pb8_tombstones"), dict) else {}
            desired_signature = (
                str(desired.get("cluster_id") or ""),
                tuple(
                    sorted(
                        (
                            str(name),
                            str(item.get("assigned_host") or ""),
                            str(item.get("config_manifest_hash") or ""),
                            bool(item.get("conflicted") is True),
                            str(item.get("desired_state") or ""),
                            str(item.get("version") or ""),
                        )
                        for name, item in instances.items()
                        if isinstance(item, dict)
                    )
                ),
                tuple(sorted(str(name) for name in tombstones)),
            )
        except OSError:
            desired_signature = (str(desired_path), None)
        except Exception as exc:
            desired_signature = (str(desired_path), "error", str(exc))

        signature: list[tuple] = [desired_signature]
        run_root = Path(self.v8_path)
        if not run_root.is_dir():
            signature.append((str(run_root), None, None))
            return tuple(signature)
        for instance_dir in sorted(run_root.iterdir(), key=lambda item: item.name):
            if not instance_dir.is_dir() or instance_dir.name.startswith(".pbgui-v8-stage-"):
                continue
            signature.append(("instance", instance_dir.name))
            for item in sorted(instance_dir.glob("*.json"), key=lambda path: path.name):
                if item.name in SYNC_EXCLUDE_FILES:
                    continue
                file_sig = self._file_signature(item)
                signature.append((instance_dir.name, item.name, file_sig[1], file_sig[2]))
        return tuple(signature)

    def has_v8_runtime_changed(self) -> bool:
        """Poll PB8 desired state and configs for immediate PBRun rescans."""

        signature = self._current_v8_runtime_signature()
        if self._v8_runtime_signature is None:
            self._v8_runtime_signature = signature
            return False
        if signature == self._v8_runtime_signature:
            return False
        self._v8_runtime_signature = signature
        _log(SERVICE, "Cluster/run_v8 state changed - rescanning PB8 instances")
        self.watch_v8()
        return True

    def fetch_cmc_credits(self):
        provider_refreshed = bool(self.coindata.fetch_api_status())
        return {
            "provider_refreshed": provider_refreshed,
            "pool": self.coindata.cmc_pool_status(),
        }

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

    def watch_v7(self, v7_instances : list = None):
        """Create or delete v7 instances and activate them or not depending on their status.

        Args:
            v7_instances (list, optional): List of v7-instance paths. Defaults to None.
        """
        if not v7_instances:
            run_root = Path(self.v7_path)
            v7_instances = [str(path) for path in sorted(run_root.iterdir(), key=lambda item: item.name)] if run_root.is_dir() else []
            active_paths = {str(Path(path)) for path in v7_instances if Path(path).is_dir()}
            for existing in list(self.run_v7):
                if str(Path(existing.path or "")) not in active_paths:
                    existing.stop()
                    self.remove_v7(existing)
        for v7_instance in v7_instances:
            file = Path(f'{v7_instance}/config.json')
            if file.exists():
                run_v7 = RunV7()
                run_v7.path = v7_instance
                run_v7.user = v7_instance.split('/')[-1]
                run_v7.name = self.name
                run_v7.pb7dir = self.pb7dir
                run_v7.pb7venv = self.pb7venv
                run_v7.pbgdir = self.pbgdir
                pre_load_gate_fn = getattr(run_v7, "_cluster_gate_result", None)
                if callable(pre_load_gate_fn):
                    pre_load_gate = pre_load_gate_fn()
                    if not pre_load_gate.get("ok") and str(pre_load_gate.get("status") or "") in CLUSTER_PRE_LOAD_BLOCK_STATES:
                        run_v7._block_cluster_gate_start(pre_load_gate, log=False)
                        if run_v7.is_running():
                            run_v7.stop()
                        self.add_v7(run_v7)
                        continue
                if run_v7.load():
                    if run_v7.is_running():
                        if not run_v7._cluster_gate_allows_run():
                            run_v7.stop()
                        else:
                            running_version = self.find_running_version(v7_instance)
                            if running_version < run_v7.version:
                                run_v7.stop()
                                run_v7.start(reload_config=False)
                    else:
                        run_v7.start(reload_config=False)
                    self.add_v7(run_v7)
                else:
                    self.remove_v7(run_v7)
                    run_v7.stop()
        self._v7_runtime_signature = self._current_v7_runtime_signature()

    def watch_v8(self, v8_instances: list | None = None) -> None:
        """Scan ``data/run_v8`` and reconcile exact PB8 live processes."""

        if not self.pb8_ready:
            for runner in list(self.run_v8):
                runner.stop()
            self.run_v8 = []
            return
        if v8_instances is None:
            run_root = Path(self.v8_path)
            v8_instances = [path for path in sorted(run_root.iterdir(), key=lambda item: item.name)] if run_root.is_dir() else []
        else:
            run_root = Path(self.v8_path)
        active_paths = {
            str(Path(path).resolve())
            for path in v8_instances
            if (
                Path(path).is_dir()
                and not Path(path).is_symlink()
                and not Path(path).name.startswith(".pbgui-v8-stage-")
                and (Path(path) / "config.json").is_file()
            )
        }
        retained: list[RunV8] = []
        existing_by_path = {str(Path(item.path).resolve()): item for item in self.run_v8}
        handled_missing_paths: set[str] = set()
        for missing_path, runner in existing_by_path.items():
            if missing_path not in active_paths:
                runner.stop()
                handled_missing_paths.add(missing_path)

        for process in psutil.process_iter():
            orphan = RunV8.from_missing_config_process(
                process,
                run_root=run_root,
                pb8dir=self.pb8dir,
                pb8venv=self.pb8venv,
                pbgdir=Path(self.pbgdir),
                name=self.name,
            )
            if orphan is None or orphan.path in handled_missing_paths:
                continue
            _log(
                SERVICE,
                f"Stop orphaned passivbot v8 process for missing config {orphan.config_path}",
                user=orphan.user,
                level="WARNING",
            )
            orphan.stop(process)
            handled_missing_paths.add(orphan.path)

        for path_text in sorted(active_paths):
            runner = existing_by_path.get(path_text) or RunV8()
            runner.path = path_text
            runner.user = Path(path_text).name
            runner.name = self.name
            runner.pb8dir = self.pb8dir
            runner.pb8venv = self.pb8venv
            runner.pbgdir = self.pbgdir
            runner.watch()
            retained.append(runner)
        self.run_v8 = retained
        self._v8_runtime_signature = self._current_v8_runtime_signature()

    def find_high_memory_bot(self):
        """Finds the bot with the highest memory usage."""
        high_mem = 0
        high_bot = None
        for runner in [*self.run_v7, *getattr(self, "run_v8", [])]:
            mem = _memory_usage_bytes(runner.memory)
            if mem > high_mem:
                high_mem = mem
                high_bot = runner
        return high_bot
    
    def watch_memory(self):
        """Watches the memory usage of the System and restart Passivbot if necessary."""
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        free = (mem.available + swap.free) / 1024 / 1024  # in MB
        if free < 250:
            high_bot = self.find_high_memory_bot()
            if high_bot:
                _log(SERVICE, f"Low System memory {free:.2f}MB, restarting bot {high_bot.user}", level="WARNING")
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
                    _log(SERVICE, "Can not start PBRun", level="ERROR")
                    break
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            _log(SERVICE, "Stop: PBRun")
            try:
                process = psutil.Process(self.my_pid)
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except psutil.TimeoutExpired:
                    _kill_process(process, "PBRun")
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
    Start and monitor configured V7 and V8 passivbot instances.
    """
    from credential_process_registry import ProcessCapabilityHeartbeat

    run = PBRun()
    if run.is_running():
        _log(SERVICE, "PBRun already started", level="ERROR")
        sys.exit(1)
    _log(SERVICE, "Start: PBRun")
    run.save_pid()
    capability = ProcessCapabilityHeartbeat(Path(run.pbgdir), "PBRun")
    capability.__enter__()
    stop_requested = threading.Event()

    def request_stop(_signum, _frame):
        stop_requested.set()

    previous_handlers = {}
    for stop_signal in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[stop_signal] = signal.signal(stop_signal, request_stop)
    try:
        _wait_for_cluster_boot_sync(Path(run.pbgdir), timeout=20)
        if run.pb7_ready:
            run.watch_v7()
        if run.pb8_ready:
            run.watch_v8()
        maintenance_count = 0
        next_maintenance = 0.0
        while not stop_requested.is_set():
            try:
                now = time()
                v7_changed, v8_changed = run.refresh_runtime_config()
                if run.pb7_ready:
                    if v7_changed:
                        run.watch_v7()
                    else:
                        # Keep Cluster Sync start/stop reactions fast without running the
                        # expensive per-bot process scan every second.
                        run.has_v7_runtime_changed()
                v8_reconciled = False
                if run.pb8_ready:
                    if v8_changed:
                        run.watch_v8()
                        v8_reconciled = True
                    else:
                        v8_reconciled = run.has_v8_runtime_changed()
                if now >= next_maintenance:
                    run.watch_memory()
                    next_maintenance = now + 5
                    for run_v7 in run.run_v7:
                        run_v7.watch()
                        run_v7.watch_dynamic()
                    if run.pb8_ready and not v8_reconciled:
                        run.watch_v8()
                    if maintenance_count % 2 == 0:
                        for run_v7 in run.run_v7:
                            run_v7.clean_log()
                    maintenance_count += 1
                stop_requested.wait(1)
            except Exception as e:
                _log(SERVICE, f"Something went wrong, but continue {e}", level="ERROR")
                _log(SERVICE, "PBRun.main loop traceback", level="DEBUG", meta={"traceback": traceback.format_exc()})
    finally:
        capability.__exit__(None, None, None)
        if run.pidfile.exists() and run.pidfile.read_text(encoding="utf-8").strip() == str(os.getpid()):
            run.pidfile.unlink(missing_ok=True)
        for stop_signal, previous_handler in previous_handlers.items():
            signal.signal(stop_signal, previous_handler)

if __name__ == '__main__':
    main()
