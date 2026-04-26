"""
api/backtest_v7.py — FastAPI backend for V7 Backtest management.

Provides:
- REST endpoints for backtest configs, queue, results, archives, settings
- WebSocket endpoint for real-time queue status updates
- Background worker (asyncio) that processes queued backtests
"""

import asyncio
import configparser
import csv
import datetime
import glob
import gzip
import io
import json
import multiprocessing
import os
import platform
import secrets
import subprocess
import time
import traceback
import uuid
from pathlib import Path, PurePath
from shutil import copytree, rmtree
from typing import Optional

import psutil
from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse

from api.auth import SessionToken, require_auth, validate_token
from api.pb7_bridge import (
    get_allowed_override_params,
    get_bot_param_keys,
    get_hsl_signal_modes,
    get_template_config,
    prepare_override_config,
)
from api.pb7_ohlcv_tools import build_ohlcv_preflight, get_ohlcv_preload_job, start_ohlcv_preload_job
from logging_helpers import human_log as _log
from pb7_config import load_pb7_config, prepare_pb7_config_dict, save_pb7_config
from pbgui_purefunc import PBGDIR, load_ini, save_ini, pb7dir, pb7venv

SERVICE = "BacktestV7API"
ARCHIVE_SERVICE = "ArchiveSync"
CLEANUP_SERVICE = "HLCVSCleanup"

# ── Draft stores for cross-page handoffs ─────────────────────────────────────
_opt_draft_store: dict[str, tuple[float, dict]] = {}
_queue_draft_store: dict[str, tuple[float, list[dict]]] = {}
_OPT_DRAFT_TTL = 600  # 10 minutes

def _clean_opt_drafts() -> None:
    now = time.time()
    for store in (_opt_draft_store, _queue_draft_store):
        for k in [k for k, v in store.items() if now - v[0] > _OPT_DRAFT_TTL]:
            del store[k]

router = APIRouter()

# ── Helpers ───────────────────────────────────────────────────

def _validate_name(name: str):
    """Reject path-traversal attempts."""
    if not name or any(c in name for c in ("/", "\\", "\x00")) or name in (".", ".."):
        raise HTTPException(400, "Invalid name")


def _editor_config_payload(cfg: dict, *, name: str | None = None) -> dict:
    """Return a Run-style editor payload with separated param status metadata."""
    if isinstance(cfg, dict):
        cfg = dict(cfg)
        backtest = cfg.get("backtest")
        if isinstance(backtest, dict):
            backtest = dict(backtest)
            backtest.pop("base_dir", None)
            cfg["backtest"] = backtest
    param_status = cfg.pop("_pbgui_param_status", {}) if isinstance(cfg, dict) else {}
    payload = {"config": cfg, "param_status": param_status}
    if name is not None:
        payload["name"] = name
    return payload


def _managed_backtest_base_dir(name: str) -> str:
    return f"backtests/pbgui/{name}"


def _normalize_backtest_base_dir(cfg: dict, name: str) -> dict:
    if not isinstance(cfg, dict):
        return cfg
    backtest = cfg.get("backtest")
    if not isinstance(backtest, dict):
        backtest = {}
        cfg["backtest"] = backtest
    backtest["base_dir"] = _managed_backtest_base_dir(name)
    return cfg


def _load_and_repair_backtest_config(name: str, cfg_file: Path) -> dict:
    try:
        cfg = load_pb7_config(cfg_file)
    except Exception as exc:
        raise HTTPException(500, f"Error reading config: {exc}") from exc

    expected_base_dir = _managed_backtest_base_dir(name)
    current_base_dir = str((cfg.get("backtest", {}) or {}).get("base_dir") or "")
    if current_base_dir != expected_base_dir:
        _normalize_backtest_base_dir(cfg, name)
        try:
            save_pb7_config(cfg, cfg_file)
        except Exception as exc:
            raise HTTPException(500, f"Error repairing config: {exc}") from exc
    return cfg


def _bt_queue_dir() -> Path:
    return Path(PBGDIR) / "data" / "bt_v7_queue"


def _bt_configs_dir() -> Path:
    return Path(PBGDIR) / "data" / "bt_v7"


def _bt_results_base() -> str:
    """Base directory for backtest results (inside pb7)."""
    return str(Path(pb7dir()) / "backtests" / "pbgui")


def _bt_results_root() -> Path:
    return Path(pb7dir()) / "backtests"


def _legacy_results_roots() -> list[Path]:
    root = _bt_results_root()
    if not root.exists():
        return []
    return [entry.resolve() for entry in sorted(root.iterdir()) if entry.is_dir() and entry.name != "pbgui"]


def _resolve_result_dir(
    path: str | Path,
    *,
    allow_pbgui: bool = True,
    allow_legacy: bool = True,
    allow_archives: bool = True,
) -> Path:
    result_dir = Path(path).resolve()
    allowed_roots: list[Path] = []
    if allow_pbgui:
        allowed_roots.append(Path(_bt_results_base()).resolve())
    if allow_legacy:
        allowed_roots.extend(_legacy_results_roots())
    if allow_archives:
        allowed_roots.append(_archives_dir().resolve())
    for root in allowed_roots:
        if result_dir.is_relative_to(root):
            return result_dir
    raise HTTPException(400, "Invalid result path")


def _find_legacy_result_root(result_dir: Path) -> Path:
    resolved = result_dir.resolve()
    for root in _legacy_results_roots():
        if resolved.is_relative_to(root):
            return root
    raise HTTPException(400, "Invalid legacy result path")


def _bt_log_dir() -> Path:
    return Path(PBGDIR) / "data" / "logs" / "backtests"


def _archives_dir() -> Path:
    return Path(PBGDIR) / "data" / "archives"


def _read_ini_section(section: str = "backtest_v7") -> dict:
    """Read backtest_v7 settings from pbgui.ini."""
    cfg = configparser.ConfigParser()
    cfg.read("pbgui.ini")
    if not cfg.has_section(section):
        return {"autostart": "False", "cpu": "1"}
    return dict(cfg.items(section))


def _write_ini(key: str, value: str, section: str = "backtest_v7"):
    save_ini(section, key, value)


# ── BacktestStore — in-memory state with change notification ──

class BacktestStore:
    """In-memory view of the backtest queue, refreshed from disk."""

    def __init__(self):
        self.items: dict[str, dict] = {}   # filename → item dict
        self.changed = asyncio.Event()
        self._lock = asyncio.Lock()

    async def refresh_from_disk(self):
        """Reload queue items from data/bt_v7_queue/*.json."""
        async with self._lock:
            dest = _bt_queue_dir()
            dest.mkdir(parents=True, exist_ok=True)
            found = {}
            for fp in sorted(dest.glob("*.json")):
                try:
                    with open(fp, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    filename = data.get("filename", fp.stem)
                    pid = self._read_pid(filename)
                    log_path = _bt_log_dir() / f"{filename}.log"
                    # Auto-migrate old log location
                    old_log = dest / f"{filename}.log"
                    if old_log.exists() and not log_path.exists():
                        log_path.parent.mkdir(parents=True, exist_ok=True)
                        old_log.rename(log_path)
                    status = self._determine_status(pid, log_path)
                    mtime = fp.stat().st_mtime
                    found[filename] = {
                        "filename": filename,
                        "name": data.get("name", filename),
                        "json": data.get("json", ""),
                        "exchange": data.get("exchange", ""),
                        "status": status,
                        "pid": pid,
                        "log_path": str(log_path),
                        "created": datetime.datetime.fromtimestamp(mtime).isoformat(),
                    }
                except Exception as e:
                    _log(SERVICE, f"Error loading queue item {fp}: {e}", level="ERROR")
            self.items = found
            self.changed.set()

    def _read_pid(self, filename: str) -> Optional[int]:
        pidfile = _bt_queue_dir() / f"{filename}.pid"
        if pidfile.exists():
            try:
                txt = pidfile.read_text().strip()
                return int(txt) if txt.isdigit() else None
            except Exception:
                return None
        return None

    def _is_process_running(self, pid: int) -> bool:
        try:
            if pid and psutil.pid_exists(pid):
                proc = psutil.Process(pid)
                return any(sub.lower().endswith("backtest.py") for sub in proc.cmdline())
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            pass
        return False

    def _determine_status(self, pid: Optional[int], log_path: Path) -> str:
        running = pid is not None and self._is_process_running(pid)
        log_tail = self._read_log_tail(log_path)

        if running:
            if log_tail and ("Backtesting " in log_tail or "Running scenario" in log_tail):
                return "backtesting"
            return "running"

        if log_tail:
            if "seconds elapsed for backtest:" in log_tail or ("Suite" in log_tail and "completed" in log_tail):
                return "complete"
            return "error"

        return "queued"

    def _read_log_tail(self, log_path: Path, size_kb: int = 50) -> Optional[str]:
        if not log_path or not log_path.exists():
            return None
        try:
            with open(log_path, "rb") as f:
                f.seek(0, 2)
                file_size = f.tell()
                start_pos = max(file_size - size_kb * 1024, 0)
                f.seek(start_pos)
                return f.read().decode("utf-8", errors="ignore")
        except Exception:
            return None

    def notify(self):
        """Signal change to WebSocket push loops."""
        self.changed.set()


_store = BacktestStore()


# ── BacktestWorker — asyncio background task ──────────────────

class BacktestWorker:
    """Processes queued backtests as an asyncio background task."""

    def __init__(self, store: BacktestStore):
        self.store = store
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self):
        if self._task is None or self._task.done():
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="backtest-worker")
            _log(SERVICE, "Backtest worker started", level="INFO")

    def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def _loop(self):
        """Main worker loop: checks queue, launches backtests respecting CPU limit."""
        try:
            while self._running:
                settings = _read_ini_section()
                autostart = settings.get("autostart", "False").lower() == "true"
                if not autostart:
                    await asyncio.sleep(5)
                    continue

                cpu_limit = min(
                    int(settings.get("cpu", "1")),
                    multiprocessing.cpu_count()
                )

                await self.store.refresh_from_disk()
                items = self.store.items

                running_count = sum(
                    1 for it in items.values() if it["status"] in ("running", "backtesting")
                )
                downloading = any(
                    it["status"] == "running" for it in items.values()
                )

                for filename, item in items.items():
                    if item["status"] != "queued":
                        continue
                    # Wait for CPU slot
                    while running_count >= cpu_limit:
                        await asyncio.sleep(3)
                        await self.store.refresh_from_disk()
                        running_count = sum(
                            1 for it in self.store.items.values()
                            if it["status"] in ("running", "backtesting")
                        )
                    # Wait for downloads to finish
                    while any(
                        it["status"] == "running" for it in self.store.items.values()
                    ):
                        await asyncio.sleep(3)
                        await self.store.refresh_from_disk()
                    # Re-check autostart
                    settings = _read_ini_section()
                    if settings.get("autostart", "False").lower() != "true":
                        break
                    # Re-check this item hasn't been removed or already started
                    if filename not in self.store.items:
                        continue
                    if self.store.items[filename]["status"] != "queued":
                        continue

                    self._launch_backtest(item)
                    _log(SERVICE, f"Launched backtest: {item['name']} ({filename})", level="INFO")
                    running_count += 1
                    await asyncio.sleep(1)
                    await self.store.refresh_from_disk()

                await asyncio.sleep(10)
        except asyncio.CancelledError:
            _log(SERVICE, "Backtest worker stopped", level="INFO")
        except Exception as e:
            _log(SERVICE, f"Backtest worker error: {e}", level="ERROR",
                 meta={"traceback": traceback.format_exc()})

    def _launch_backtest(self, item: dict):
        """Spawn a backtest subprocess (detached)."""
        venv = pb7venv()
        pb7 = pb7dir()
        config_path = item["json"]
        filename = item["filename"]

        cmd = [venv, "-u", str(PurePath(f"{pb7}/src/backtest.py")), str(PurePath(config_path))]
        log_path = _bt_log_dir() / f"{filename}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = open(log_path, "w")

        old_path = os.environ.get("PATH", "")
        new_path = os.path.dirname(venv) + os.pathsep + old_path
        env = os.environ.copy()
        env["PATH"] = new_path

        if platform.system() == "Windows":
            flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW
            proc = subprocess.Popen(
                cmd, stdout=log_file, stderr=log_file,
                cwd=pb7, text=True, creationflags=flags, env=env
            )
        else:
            proc = subprocess.Popen(
                cmd, stdout=log_file, stderr=log_file,
                cwd=pb7, text=True, start_new_session=True, env=env
            )

        # Write PID file
        pidfile = _bt_queue_dir() / f"{filename}.pid"
        pidfile.write_text(str(proc.pid))


_worker = BacktestWorker(_store)


# ── ArchiveSyncWorker — auto-pull all archives ─────────────────

def _log_archive(msg: str, level: str = "INFO"):
    """Write to ArchiveSync.log, then also route via _log for normal log infrastructure."""
    _log(ARCHIVE_SERVICE, msg, level=level)


def _read_auto_pull_interval() -> int:
    """Return auto-pull interval in minutes (0 = disabled)."""
    val = load_ini("config_archive", "auto_pull_interval") or "0"
    try:
        return max(0, int(val))
    except (ValueError, TypeError):
        return 0


def _pull_all_archives_sync() -> list[dict]:
    """Pull all cloned archives; returns list of {name, output, error} dicts."""
    base = _archives_dir()
    results = []
    if not base.exists():
        return results
    for d in sorted(base.iterdir()):
        if not (d / ".git" / "config").exists():
            continue
        name = d.name
        try:
            result = subprocess.run(
                ["git", "pull"], cwd=str(d),
                capture_output=True, text=True, timeout=60
            )
            output = (result.stdout + result.stderr).strip()
            _log_archive(f"[{name}] git pull: {output or 'ok'}")
            results.append({"name": name, "output": output, "error": ""})
        except subprocess.TimeoutExpired:
            _log_archive(f"[{name}] git pull timed out", level="ERROR")
            results.append({"name": name, "output": "", "error": "timed out"})
        except Exception as exc:
            _log_archive(f"[{name}] git pull failed: {exc}", level="ERROR")
            results.append({"name": name, "output": "", "error": str(exc)})
    return results


class ArchiveSyncWorker:
    """Background asyncio task: periodically pulls all archives."""

    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self):
        if self._task is None or self._task.done():
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="archive-sync-worker")

    def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def _loop(self):
        try:
            while self._running:
                interval = _read_auto_pull_interval()
                if interval <= 0:
                    await asyncio.sleep(60)
                    continue
                _log_archive(f"Auto-pulling all archives (interval={interval}min)…")
                await asyncio.get_event_loop().run_in_executor(None, _pull_all_archives_sync)
                # Sleep interval minutes, checking for stop/config changes every 30s
                remaining = interval * 60
                while remaining > 0 and self._running:
                    await asyncio.sleep(min(30, remaining))
                    remaining -= 30
                    new_interval = _read_auto_pull_interval()
                    if new_interval != interval:
                        break
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            _log_archive(f"ArchiveSyncWorker error: {exc}", level="ERROR")


_archive_sync_worker = ArchiveSyncWorker()


# ── WebSocket ─────────────────────────────────────────────────

_ws_clients: set[WebSocket] = set()

# ── Archive inotify watcher ────────────────────────────────────
# Uses raw Linux inotify via ctypes (same approach as master/v7_config_sync.py).
# Watches the archives directory tree for IN_CREATE / IN_MOVED_TO / IN_DELETE
# events and signals connected WS clients so they can refresh without polling.

import ctypes
import ctypes.util
import struct

_libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)

# inotify event flags
_IN_CREATE   = 0x00000100
_IN_DELETE   = 0x00000200
_IN_MOVED_TO = 0x00000080
_IN_MOVE_SELF= 0x00000800
_INOTIFY_MASK = _IN_CREATE | _IN_DELETE | _IN_MOVED_TO | _IN_MOVE_SELF
_INOTIFY_EVENT_STRUCT = struct.Struct("iIII")  # wd, mask, cookie, len

_archive_watcher_task: asyncio.Task | None = None


async def _archive_watcher_loop() -> None:
    """inotify-based watcher for the local archives directory.

    Adds watches on the archives root and all immediate subdirectories
    (one level = one archive, each containing per-config subdirs).
    When files are created/deleted/moved in the tree, sets
    ``_archive_changed`` so the WS push loop can broadcast an
    ``archive_update`` message to all connected clients.
    """
    loop = asyncio.get_running_loop()
    fd: int = -1
    try:
        fd = _libc.inotify_init1(0o00004000)  # IN_NONBLOCK = O_NONBLOCK
        if fd < 0:
            _log(SERVICE, "inotify_init1 failed — archive watcher disabled", level="WARNING")
            return

        def _add_watch(path: str) -> int:
            return _libc.inotify_add_watch(fd, path.encode(), _INOTIFY_MASK)

        # Watch root + all existing subdirs (recursion depth 1 is enough:
        # archives/<name>/<config>/<timestamp>/analysis.json)
        watched: dict[int, str] = {}  # wd → path

        def _setup_watches() -> None:
            watched.clear()
            root = _archives_dir()
            root.mkdir(parents=True, exist_ok=True)
            wd = _add_watch(str(root))
            if wd >= 0:
                watched[wd] = str(root)
            # Watch each archive dir and each config dir inside it
            for archive_dir in root.iterdir():
                if not archive_dir.is_dir():
                    continue
                wd = _add_watch(str(archive_dir))
                if wd >= 0:
                    watched[wd] = str(archive_dir)
                for cfg_dir in archive_dir.iterdir():
                    if not cfg_dir.is_dir():
                        continue
                    wd = _add_watch(str(cfg_dir))
                    if wd >= 0:
                        watched[wd] = str(cfg_dir)

        _setup_watches()

        _log(SERVICE, f"Archive inotify watcher started ({len(watched)} watches)", level="DEBUG")

        reader_fd = os.fdopen(fd, "rb", buffering=0, closefd=False)
        ev_size = _INOTIFY_EVENT_STRUCT.size

        def _readable() -> bytes:
            return reader_fd.read(4096)

        while True:
            # Wait for data on fd using asyncio's add_reader
            data_ready = asyncio.Event()
            loop.add_reader(fd, data_ready.set)
            try:
                await data_ready.wait()
            finally:
                loop.remove_reader(fd)

            try:
                raw = _readable()
            except BlockingIOError:
                continue

            pos = 0
            needs_rewwatch = False
            while pos + ev_size <= len(raw):
                wd, mask, _cookie, name_len = _INOTIFY_EVENT_STRUCT.unpack_from(raw, pos)
                pos += ev_size + name_len
                if mask & (_IN_CREATE | _IN_MOVED_TO | _IN_DELETE | _IN_MOVE_SELF):
                    # Broadcast archive_update directly to all connected WS clients
                    for _client in list(_ws_clients):
                        try:
                            await _client.send_json({"type": "archive_update"})
                        except Exception:
                            pass
                    # New subdirectory created → add watch for it
                    if mask & (_IN_CREATE | _IN_MOVED_TO):
                        needs_rewwatch = True

            if needs_rewwatch:
                # Re-scan watches after short delay (dir may not be fully created yet)
                await asyncio.sleep(0.2)
                _setup_watches()

    except asyncio.CancelledError:
        pass
    except Exception as e:
        _log(SERVICE, f"Archive watcher error: {e}", level="WARNING")
    finally:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass
        _log(SERVICE, "Archive inotify watcher stopped", level="DEBUG")


async def _ws_push_loop(ws: WebSocket):
    """Push queue state to a single WebSocket client on changes."""
    try:
        while True:
            try:
                await _store.refresh_from_disk()
                _store.changed.clear()   # clear AFTER refresh (refresh sets the event)
                msg = {
                    "type": "queue_update",
                    "items": list(_store.items.values()),
                    "settings": _read_ini_section(),
                }
                await ws.send_json(msg)
            except (WebSocketDisconnect, RuntimeError):
                break
            except Exception as e:
                _log(SERVICE, f"WS push error: {e}", level="WARNING")
            # Wait for next change or poll every 3 seconds
            try:
                await asyncio.wait_for(_store.changed.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                pass
    except asyncio.CancelledError:
        pass


@router.websocket("/ws/bt7")
async def ws_backtest(websocket: WebSocket):
    token = websocket.query_params.get("token", "")
    if not validate_token(token):
        await websocket.close(code=4001)
        return
    await websocket.accept()
    _ws_clients.add(websocket)
    push_task = asyncio.create_task(_ws_push_loop(websocket))
    try:
        while True:
            data = await websocket.receive_text()
            # Handle client messages (e.g. request refresh)
            try:
                msg = json.loads(data)
                if msg.get("type") == "refresh":
                    await _store.refresh_from_disk()
                    _store.notify()
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)
        push_task.cancel()
        try:
            await push_task
        except asyncio.CancelledError:
            pass


# ── HLCVS Cache Cleanup Worker ────────────────────────────────

class HLCVSCleanupWorker:
    """Periodically removes old hlcvs_data directories to free disk space."""

    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self):
        if self._task is None or self._task.done():
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="hlcvs-cleanup")
            _log(CLEANUP_SERVICE, "HLCVS cleanup worker started", level="INFO")

    def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def _loop(self):
        try:
            while self._running:
                try:
                    settings = _read_ini_section()
                    enabled = settings.get("hlcvs_cleanup_enabled", "False").lower() == "true"
                    interval_h = max(1, int(settings.get("hlcvs_cleanup_interval_h", "24")))
                    if enabled:
                        days = max(1, int(settings.get("hlcvs_cleanup_days", "7")))
                        await asyncio.to_thread(self._do_cleanup, days)
                    await asyncio.sleep(interval_h * 3600)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    _log(CLEANUP_SERVICE, f"Error in cleanup loop: {e}",
                         level="ERROR", meta={"traceback": traceback.format_exc()})
                    await asyncio.sleep(300)
        except asyncio.CancelledError:
            pass

    @staticmethod
    def _do_cleanup(retention_days: int):
        hlcvs_dir = Path(pb7dir()) / "caches" / "hlcvs_data"
        if not hlcvs_dir.is_dir():
            return
        cutoff = datetime.datetime.now().timestamp() - (retention_days * 86400)
        removed = 0
        errors = 0
        for entry in hlcvs_dir.iterdir():
            if not entry.is_dir():
                continue
            try:
                mtime = entry.stat().st_mtime
                if mtime < cutoff:
                    rmtree(entry)
                    removed += 1
            except Exception as e:
                errors += 1
                _log(CLEANUP_SERVICE, f"Failed to remove {entry.name}: {e}", level="WARNING")
        if removed > 0:
            _log(CLEANUP_SERVICE,
                 f"Cleaned {removed} hlcvs_data dirs older than {retention_days}d"
                 + (f" ({errors} errors)" if errors else ""),
                 level="INFO")


_hlcvs_cleanup_worker = HLCVSCleanupWorker()


# ── Lifespan hook ─────────────────────────────────────────────

def startup():
    """Called from PBApiServer lifespan to start the worker."""
    global _archive_watcher_task
    _worker.start()
    _archive_sync_worker.start()
    _hlcvs_cleanup_worker.start()
    _archive_watcher_task = asyncio.create_task(
        _archive_watcher_loop(), name="archive-inotify-watcher"
    )


def shutdown():
    """Called from PBApiServer lifespan to stop the worker."""
    global _archive_watcher_task
    _worker.stop()
    _archive_sync_worker.stop()
    _hlcvs_cleanup_worker.stop()
    if _archive_watcher_task and not _archive_watcher_task.done():
        _archive_watcher_task.cancel()


# ── REST: Main page ───────────────────────────────────────────

@router.get("/main_page", response_class=HTMLResponse)
def main_page(
    request: Request,
    st_base: str = Query(default="", description="Streamlit base URL"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    html_path = Path(__file__).resolve().parent.parent / "frontend" / "v7_backtest.html"
    if not html_path.exists():
        raise HTTPException(404, "v7_backtest.html not found")
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/backtest-v7"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))

    if not st_base:
        st_base = f"http://{host}:8501"
    html = html.replace('"%%ST_BASE%%"', json.dumps(st_base))

    from pbgui_purefunc import PBGUI_VERSION, PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).resolve().parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


# ── REST: Optimize draft (optimize-from-result) ──────────────

@router.post("/optimize-draft")
def create_optimize_draft(body: dict, session: SessionToken = Depends(require_auth)):
    """Store a config dict as a short-lived draft for the Optimize editor."""
    _clean_opt_drafts()
    config = body.get("config")
    if not isinstance(config, dict):
        raise HTTPException(status_code=422, detail="config must be a dict")
    draft_id = secrets.token_urlsafe(16)
    _opt_draft_store[draft_id] = (time.time(), config)
    return {"draft_id": draft_id}


@router.get("/optimize-draft/{draft_id}")
def get_optimize_draft(draft_id: str, session: SessionToken = Depends(require_auth)):
    """Retrieve a previously stored optimize draft."""
    entry = _opt_draft_store.get(draft_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Draft not found or expired")
    return {"config": entry[1]}


@router.post("/queue-draft")
def create_queue_draft(body: dict, session: SessionToken = Depends(require_auth)):
    """Store multiple backtest configs as a short-lived draft for queue parameter selection."""
    _clean_opt_drafts()
    items = body.get("items")
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=422, detail="items must be a non-empty list")

    normalized = []
    for item in items:
        if not isinstance(item, dict):
            raise HTTPException(status_code=422, detail="each item must be an object")
        config = item.get("config")
        if not isinstance(config, dict):
            raise HTTPException(status_code=422, detail="each item.config must be a dict")
        name = str(item.get("name") or "rebacktest")
        normalized.append({"name": name, "config": config})

    draft_id = secrets.token_urlsafe(16)
    _queue_draft_store[draft_id] = (time.time(), normalized)
    return {"draft_id": draft_id}


@router.get("/queue-draft/{draft_id}")
def get_queue_draft(draft_id: str, session: SessionToken = Depends(require_auth)):
    """Retrieve a previously stored backtest queue draft."""
    entry = _queue_draft_store.get(draft_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Draft not found or expired")
    return {"items": entry[1]}


# ── REST: PBGui data path ─────────────────────────────────────

@router.get("/pbgui_data_path")
def get_pbgui_data_path(session: SessionToken = Depends(require_auth)):
    """Return the PBGui-managed market data root directory."""
    from market_data import get_market_data_root_dir
    return {"path": str(get_market_data_root_dir())}


@router.post("/ohlcv-preflight")
async def get_ohlcv_preflight(body: dict, session: SessionToken = Depends(require_auth)):
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="Missing or invalid 'config' in body")
    try:
        return await build_ohlcv_preflight(config)
    except HTTPException:
        raise
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(
            SERVICE,
            f"Failed to build OHLCV preflight: {detail}",
            level="WARNING",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=422, detail=detail) from exc


@router.post("/ohlcv-preload")
def start_editor_ohlcv_preload(body: dict, session: SessionToken = Depends(require_auth)):
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="Missing or invalid 'config' in body")
    try:
        return start_ohlcv_preload_job(config)
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(
            SERVICE,
            f"Failed to start OHLCV preload: {detail}",
            level="WARNING",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=422, detail=detail) from exc


@router.get("/ohlcv-preload/{job_id}")
def get_editor_ohlcv_preload(job_id: str, session: SessionToken = Depends(require_auth)):
    payload = get_ohlcv_preload_job(job_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="OHLCV preload job not found")
    return payload


# ── REST: Settings ────────────────────────────────────────────

@router.get("/settings")
def get_settings(session: SessionToken = Depends(require_auth)):
    settings = _read_ini_section()
    cpu_max = multiprocessing.cpu_count()
    return {
        "autostart": settings.get("autostart", "False").lower() == "true",
        "cpu": min(int(settings.get("cpu", "1")), cpu_max),
        "cpu_max": cpu_max,
        "hsl_signal_modes": get_hsl_signal_modes(),
        "hlcvs_cleanup_enabled": settings.get("hlcvs_cleanup_enabled", "False").lower() == "true",
        "hlcvs_cleanup_days": int(settings.get("hlcvs_cleanup_days", "7")),
        "hlcvs_cleanup_interval_h": int(settings.get("hlcvs_cleanup_interval_h", "24")),
    }


@router.post("/settings")
def update_settings(body: dict, session: SessionToken = Depends(require_auth)):
    if "autostart" in body:
        _write_ini("autostart", str(bool(body["autostart"])))
    if "cpu" in body:
        cpu = max(1, min(int(body["cpu"]), multiprocessing.cpu_count()))
        _write_ini("cpu", str(cpu))
    if "hlcvs_cleanup_enabled" in body:
        _write_ini("hlcvs_cleanup_enabled", str(bool(body["hlcvs_cleanup_enabled"])))
    if "hlcvs_cleanup_days" in body:
        days = max(1, min(int(body["hlcvs_cleanup_days"]), 365))
        _write_ini("hlcvs_cleanup_days", str(days))
    if "hlcvs_cleanup_interval_h" in body:
        interval = max(1, min(int(body["hlcvs_cleanup_interval_h"]), 168))
        _write_ini("hlcvs_cleanup_interval_h", str(interval))
    _store.notify()
    return {"ok": True}


@router.post("/settings/hlcvs-cleanup-now")
async def hlcvs_cleanup_now(body: dict, session: SessionToken = Depends(require_auth)):
    """Trigger an immediate HLCVS cache cleanup."""
    days = max(1, min(int(body.get("days", 7)), 365))
    result = await asyncio.to_thread(_hlcvs_cleanup_now_sync, days)
    return result


def _hlcvs_cleanup_now_sync(retention_days: int) -> dict:
    hlcvs_dir = Path(pb7dir()) / "caches" / "hlcvs_data"
    if not hlcvs_dir.is_dir():
        return {"removed": 0, "freed_mb": 0}
    import time
    cutoff = time.time() - (retention_days * 86400)
    removed = 0
    freed = 0
    for entry in hlcvs_dir.iterdir():
        if not entry.is_dir():
            continue
        try:
            if entry.stat().st_mtime < cutoff:
                size = sum(f.stat().st_size for f in entry.rglob("*") if f.is_file())
                rmtree(entry)
                removed += 1
                freed += size
        except Exception as e:
            _log(CLEANUP_SERVICE, f"Failed to remove {entry.name}: {e}", level="WARNING")
    if removed > 0:
        _log(CLEANUP_SERVICE,
             f"Manual cleanup: removed {removed} dirs older than {retention_days}d, freed {freed // (1024*1024)} MB",
             level="INFO")
    return {"removed": removed, "freed_mb": round(freed / (1024 * 1024))}


# ── REST: Bot params (from passivbot schema) ─────────────────

@router.get("/configs/new-config")
def get_new_backtest_config(session: SessionToken = Depends(require_auth)):
    """Return a default backtest config from the passivbot schema.

    Using get_template_config() keeps the defaults always in sync with the
    installed passivbot version without any manual maintenance.
    """
    try:
        tmpl = get_template_config()
    except Exception as exc:
        _log(SERVICE, f"Failed to load template config: {exc}", level="warning")
        tmpl = {"backtest": {}, "bot": {}, "live": {}, "optimize": {}}
    return _editor_config_payload(tmpl)


@router.get("/bot-params")
def get_bot_params(session: SessionToken = Depends(require_auth)):
    """Return list of bot.long parameter names from passivbot schema."""
    try:
        return {"params": [{"key": key} for key in get_bot_param_keys()]}
    except Exception as exc:
        _log(SERVICE, f"Failed to load bot params: {exc}", level="warning")
        return {"params": []}


@router.get("/override-params")
def get_override_params(session: SessionToken = Depends(require_auth)):
    """Return allowed coin_overrides parameters from passivbot."""
    try:
        return {"params": get_allowed_override_params()}
    except Exception as exc:
        _log(SERVICE, f"Failed to load override params: {exc}", level="warning")
        return {"params": {}}


@router.get("/override-config/{config_name}/{filename}")
def get_override_config(config_name: str, filename: str,
                        session: SessionToken = Depends(require_auth)):
    """Read an override config file (e.g. 1000BONKUSDT.json) from a config directory."""
    _validate_name(config_name)
    # Sanitize filename — only allow simple filenames, no path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(400, "Invalid filename")
    cfg_dir = _bt_configs_dir() / config_name
    override_file = cfg_dir / filename
    if not override_file.exists():
        # Fallback: find pre-normalization file (e.g. BONK.json → 1000BONKUSDT.json)
        stem = filename.rsplit(".", 1)[0] if "." in filename else filename
        norm = _normalize_coin_name(stem)
        for f in cfg_dir.iterdir():
            if f.suffix == ".json" and f.name != "backtest.json":
                if _normalize_coin_name(f.stem) == norm:
                    override_file = f
                    break
    if not override_file.exists():
        raise HTTPException(404, f"Override config '{filename}' not found in '{config_name}'")
    try:
        with open(override_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {"config": prepare_override_config(data, verbose=False)}
    except Exception as exc:
        raise HTTPException(500, f"Error reading override config: {exc}")


@router.put("/override-config/{config_name}/{filename}")
def save_override_config(config_name: str, filename: str, body: dict,
                         session: SessionToken = Depends(require_auth)):
    """Save an override config file (e.g. HYPE.json) to a config directory.

    The request body contains only the override params
    ({bot: {long: {...}, short: {...}}, live: {...}}).
    Written as-is — override files are sparse diffs, not full configs.
    """
    _validate_name(config_name)
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(400, "Invalid filename")
    if not filename.endswith(".json"):
        raise HTTPException(400, "Filename must end with .json")
    cfg_dir = _bt_configs_dir() / config_name
    cfg_dir.mkdir(parents=True, exist_ok=True)
    override_file = cfg_dir / filename
    # Ensure ``live`` key exists so passivbot's load_prepared_config can
    # detect the "live_only" flavor when loading the file at backtest time.
    if "live" not in body:
        body["live"] = {}
    tmp = override_file.with_suffix(".json.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(body, f, indent=4)
            f.write("\n")
        os.replace(str(tmp), str(override_file))
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise
    return {"ok": True}


# ── REST: Configs (saved backtest configurations) ─────────────

@router.get("/configs")
def list_configs(session: SessionToken = Depends(require_auth)):
    """List saved backtest configs from data/bt_v7/*/backtest.json."""
    base = _bt_configs_dir()
    configs = []
    if base.exists():
        for p in sorted(base.iterdir()):
            cfg_file = p / "backtest.json"
            if cfg_file.exists():
                try:
                    with open(cfg_file, "r", encoding="utf-8") as f:
                        cfg = json.load(f)
                    # Count results
                    results_path = Path(_bt_results_base()) / p.name
                    result_count = len(list(results_path.glob("**/analysis.json"))) if results_path.exists() else 0
                    # Extract key info
                    bt = cfg.get("backtest", {})
                    bot = cfg.get("bot", {})
                    live = cfg.get("live", {})
                    exchanges = bt.get("exchanges", [])
                    approved_long = live.get("approved_coins", {}).get("long", [])
                    approved_short = live.get("approved_coins", {}).get("short", [])
                    coins = list(set(approved_long + approved_short))
                    configs.append({
                        "name": p.name,
                        "exchanges": exchanges,
                        "coins": len(coins),
                        "coin_list": coins,
                        "results": result_count,
                        "start_date": bt.get("start_date", ""),
                        "end_date": bt.get("end_date", ""),
                        "starting_balance": bt.get("starting_balance", 0),
                        "twe_long": bot.get("long", {}).get("total_wallet_exposure_limit", 0),
                        "twe_short": bot.get("short", {}).get("total_wallet_exposure_limit", 0),
                        "pos_long": bot.get("long", {}).get("n_positions", 0),
                        "pos_short": bot.get("short", {}).get("n_positions", 0),
                        "modified": datetime.datetime.fromtimestamp(cfg_file.stat().st_mtime).isoformat(),
                    })
                except Exception as e:
                    _log(SERVICE, f"Error reading config {cfg_file}: {e}", level="WARNING")
    return {"configs": configs}


@router.get("/configs/{name}")
def get_config(name: str, session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    cfg_file = _bt_configs_dir() / name / "backtest.json"
    if not cfg_file.exists():
        raise HTTPException(404, f"Config '{name}' not found")
    try:
        cfg = load_pb7_config(cfg_file, neutralize_added=True)
        return _editor_config_payload(cfg, name=name)
    except HTTPException:
        raise
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(
            SERVICE,
            f"Failed to load backtest config '{name}': {detail}",
            level="WARNING",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=422, detail=detail)


@router.post("/configs/prepare")
def prepare_config_for_editor(body: dict, session: SessionToken = Depends(require_auth)):
    """Normalize an in-memory config dict for Backtest editor import flows."""
    cfg = body.get("config") if isinstance(body, dict) else None
    if not isinstance(cfg, dict):
        raise HTTPException(status_code=400, detail="Missing or invalid 'config' in body")
    try:
        prepared = prepare_pb7_config_dict(cfg, neutralize_added=True)
        return _editor_config_payload(prepared)
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(
            SERVICE,
            f"Failed to prepare imported backtest config: {detail}",
            level="WARNING",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=422, detail=detail) from exc


def _normalize_coin_name(symbol: str) -> str:
    """Normalize exchange symbol to short coin name (mirrors JS _covNormalizeCoin).
    E.g. HYPEUSDT → HYPE, 1000BONKUSDT → BONK, kSHIB → SHIB."""
    import re
    s = symbol.upper()
    for q in ('USDT', 'USDC', 'BUSD', 'USD'):
        if len(s) > len(q) and s.endswith(q):
            s = s[:-len(q)]
            break
    m = re.match(r'^(10+)([A-Z].*)', s)
    if m:
        s = m.group(2)
    if len(s) > 1 and s[0] == 'K' and s[1] != 'K':
        tail = s[1:]
        if re.match(r'^[A-Z]+$', tail):
            s = tail
    return s


def _copy_override_files(cfg: dict, src_dir: Path, dst_dir: Path) -> None:
    """Copy override_config_path files referenced in coin_overrides.
    Handles normalized filenames: if HYPE.json is referenced but only
    HYPEUSDT.json exists in src, copies it with the new name."""
    import shutil
    overrides = cfg.get("coin_overrides", {})
    # Build reverse map: normalized coin name → source file on disk
    src_file_map = {}
    if src_dir.is_dir():
        for f in src_dir.iterdir():
            if f.suffix == ".json" and f.name != "backtest.json":
                norm = _normalize_coin_name(f.stem)
                src_file_map[norm] = f
    for coin, ov in overrides.items():
        fname = ov.get("override_config_path", "")
        if not fname:
            continue
        safe = Path(fname).name  # prevent path traversal
        src_file = src_dir / safe
        if not src_file.is_file():
            # Fallback: find source file via normalization
            norm = _normalize_coin_name(coin)
            src_file = src_file_map.get(norm)
        if src_file and src_file.is_file():
            shutil.copy2(str(src_file), str(dst_dir / safe))


@router.put("/configs/{name}")
def save_config(name: str, body: dict, source_name: str = None,
               session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    cfg = _normalize_backtest_base_dir(body, name)
    cfg_dir = _bt_configs_dir() / name
    cfg_dir.mkdir(parents=True, exist_ok=True)
    # Copy override_config_path files from source when saving as new name
    if source_name and source_name != name:
        _validate_name(source_name)
        _copy_override_files(cfg, _bt_configs_dir() / source_name, cfg_dir)
    cfg_file = cfg_dir / "backtest.json"
    save_pb7_config(cfg, cfg_file)
    # Rename pre-normalization override files and delete truly orphaned ones
    _cleanup_orphaned_overrides(cfg, cfg_dir)
    return {"ok": True, "name": name}


def _cleanup_orphaned_overrides(cfg: dict, cfg_dir: Path) -> None:
    """Rename pre-normalization override files (e.g. HYPEUSDT.json → HYPE.json),
    ensure every referenced override file contains ``live`` key (required for
    passivbot's flavor detection), and delete truly orphaned .json files."""
    referenced = set()
    for coin, ov in cfg.get("coin_overrides", {}).items():
        fname = ov.get("override_config_path", "")
        if fname:
            referenced.add(Path(fname).name)
    for f in list(cfg_dir.iterdir()):
        if f.suffix != ".json" or f.name == "backtest.json":
            continue
        if f.name in referenced:
            # Ensure ``live`` key exists for passivbot flavor detection
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                if "live" not in data:
                    data["live"] = {}
                    tmp = f.with_suffix(".json.tmp")
                    with open(tmp, "w", encoding="utf-8") as fh:
                        json.dump(data, fh, indent=4)
                        fh.write("\n")
                    os.replace(str(tmp), str(f))
            except (OSError, json.JSONDecodeError):
                pass
            continue  # keep referenced file
        # Check if this is a pre-normalization file that should be renamed
        norm_fname = _normalize_coin_name(f.stem) + ".json"
        if norm_fname in referenced and not (cfg_dir / norm_fname).exists():
            f.rename(cfg_dir / norm_fname)
        else:
            f.unlink(missing_ok=True)


@router.delete("/configs/{name}")
def delete_config(name: str, remove_results: bool = False,
                  session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    cfg_dir = _bt_configs_dir() / name
    if not cfg_dir.exists():
        raise HTTPException(404, f"Config '{name}' not found")
    rmtree(str(cfg_dir), ignore_errors=True)
    if remove_results:
        results_dir = Path(_bt_results_base()) / name
        if results_dir.exists():
            rmtree(str(results_dir), ignore_errors=True)
    return {"ok": True}


@router.post("/configs/{name}/duplicate")
def duplicate_config(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    new_name = body.get("new_name", "")
    _validate_name(new_name)
    src_dir = _bt_configs_dir() / name
    if not (src_dir / "backtest.json").exists():
        raise HTTPException(404, f"Config '{name}' not found")
    dst_dir = _bt_configs_dir() / new_name
    if dst_dir.exists():
        raise HTTPException(409, f"Config '{new_name}' already exists")
    import shutil
    shutil.copytree(str(src_dir), str(dst_dir))
    dst_cfg_file = dst_dir / "backtest.json"
    try:
        cfg = load_pb7_config(dst_cfg_file)
        _normalize_backtest_base_dir(cfg, new_name)
        save_pb7_config(cfg, dst_cfg_file)
    except Exception as exc:
        rmtree(str(dst_dir), ignore_errors=True)
        raise HTTPException(500, f"Failed to duplicate config: {exc}") from exc
    return {"ok": True, "name": new_name}


# ── REST: Queue ───────────────────────────────────────────────

@router.get("/queue")
def get_queue(session: SessionToken = Depends(require_auth)):
    """Get current queue items with status."""
    # Synchronous refresh for REST
    items = _load_queue_sync()
    return {"items": items}


def _load_queue_sync() -> list[dict]:
    """Load queue items from disk (sync version for REST endpoints)."""
    dest = _bt_queue_dir()
    dest.mkdir(parents=True, exist_ok=True)
    items = []
    for fp in sorted(dest.glob("*.json")):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            filename = data.get("filename", fp.stem)
            pid = _store._read_pid(filename)
            log_path = _bt_log_dir() / f"{filename}.log"
            status = _store._determine_status(pid, log_path)
            items.append({
                "filename": filename,
                "name": data.get("name", filename),
                "json": data.get("json", ""),
                "exchange": data.get("exchange", ""),
                "status": status,
                "pid": pid,
                "log_path": str(log_path),
                "created": datetime.datetime.fromtimestamp(fp.stat().st_mtime).isoformat(),
            })
        except Exception as e:
            _log(SERVICE, f"Error loading queue item {fp}: {e}", level="WARNING")
    return items


@router.post("/queue")
def add_to_queue(body: dict, session: SessionToken = Depends(require_auth)):
    """Add a backtest config to the queue.

    Body: {name} or {name, config}.
    Without ``config``: uses the existing config at data/bt_v7/{name}/backtest.json
    directly (override files sit next to it, same as Streamlit).
    With ``config``: saves to data/bt_v7/{name}/backtest.json first (for re-backtest
    from results with modified params).
    """
    name = body.get("name", "")
    if not name:
        raise HTTPException(400, "name is required")
    _validate_name(name)

    cfg_dir = _bt_configs_dir() / name
    cfg_file = cfg_dir / "backtest.json"

    # If config body provided, save it first (re-backtest scenario)
    config = body.get("config")
    if config:
        cfg_dir.mkdir(parents=True, exist_ok=True)
        _normalize_backtest_base_dir(config, name)
        save_pb7_config(config, cfg_file)

    if not cfg_file.exists():
        raise HTTPException(404, f"Config '{name}' not found")

    # Repair older configs created before base_dir normalization before queueing.
    cfg = _load_and_repair_backtest_config(name, cfg_file)

    filename = str(uuid.uuid4())
    bt = cfg.get("backtest", {})
    exchanges = bt.get("exchanges", [])
    exchange_str = exchanges if isinstance(exchanges, list) else [exchanges]

    # Save queue metadata — json points to the original config file
    queue_dir = _bt_queue_dir()
    queue_dir.mkdir(parents=True, exist_ok=True)
    queue_file = queue_dir / f"{filename}.json"
    queue_data = {
        "name": name,
        "filename": filename,
        "json": str(cfg_file),
        "exchange": exchange_str,
    }
    with open(queue_file, "w", encoding="utf-8") as f:
        json.dump(queue_data, f, indent=4)

    _store.notify()
    return {"ok": True, "filename": filename}


@router.post("/queue/{filename}/start")
def start_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    """Manually start a single queued backtest."""
    _validate_name(filename)
    queue_file = _bt_queue_dir() / f"{filename}.json"
    if not queue_file.exists():
        raise HTTPException(404, "Queue item not found")

    with open(queue_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    item = {
        "filename": filename,
        "name": data.get("name", filename),
        "json": data.get("json", ""),
        "exchange": data.get("exchange", ""),
    }
    _worker._launch_backtest(item)
    _store.notify()
    return {"ok": True}


@router.post("/queue/{filename}/restart")
def restart_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    """Reset an errored queue item and launch it immediately."""
    _validate_name(filename)
    queue_file = _bt_queue_dir() / f"{filename}.json"
    if not queue_file.exists():
        raise HTTPException(404, "Queue item not found")
    with open(queue_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    data["status"] = "queued"
    data.pop("error", None)
    with open(queue_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    # Launch directly — don't rely on autostart being enabled
    item = {
        "filename": filename,
        "name": data.get("name", filename),
        "json": data.get("json", ""),
        "exchange": data.get("exchange", ""),
    }
    _worker._launch_backtest(item)
    _store.notify()
    return {"ok": True}


@router.post("/queue/{filename}/stop")
def stop_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    """Stop a running backtest."""
    _validate_name(filename)
    pid = _store._read_pid(filename)
    if pid and _store._is_process_running(pid):
        try:
            psutil.Process(pid).kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    _store.notify()
    return {"ok": True}


@router.delete("/queue/{filename}")
def remove_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    """Remove a queue item (stops if running)."""
    _validate_name(filename)
    # Stop if running
    pid = _store._read_pid(filename)
    if pid and _store._is_process_running(pid):
        try:
            psutil.Process(pid).kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    # Remove files
    (_bt_queue_dir() / f"{filename}.json").unlink(missing_ok=True)
    (_bt_queue_dir() / f"{filename}.pid").unlink(missing_ok=True)
    (_bt_log_dir() / f"{filename}.log").unlink(missing_ok=True)
    _store.notify()
    return {"ok": True}


@router.post("/queue/clear-finished")
def clear_finished(session: SessionToken = Depends(require_auth)):
    """Remove all finished queue items."""
    items = _load_queue_sync()
    removed = 0
    for item in items:
        if item["status"] == "complete":
            fn = item["filename"]
            (_bt_queue_dir() / f"{fn}.json").unlink(missing_ok=True)
            (_bt_queue_dir() / f"{fn}.pid").unlink(missing_ok=True)
            (_bt_log_dir() / f"{fn}.log").unlink(missing_ok=True)
            removed += 1
    _store.notify()
    return {"ok": True, "removed": removed}


@router.get("/queue/{filename}/log")
def get_queue_log(filename: str, lines: int = 100,
                  session: SessionToken = Depends(require_auth)):
    """Get tail of a backtest log."""
    _validate_name(filename)
    log_path = _bt_log_dir() / f"{filename}.log"
    if not log_path.exists():
        return {"log": "", "exists": False}
    tail = _store._read_log_tail(log_path, size_kb=max(10, lines))
    return {"log": tail or "", "exists": True}


# ── REST: Results ─────────────────────────────────────────────

@router.get("/results")
def list_results(name: str = None, session: SessionToken = Depends(require_auth)):
    """List backtest results. If name given, only for that config."""
    base = Path(_bt_results_base())
    if not base.exists():
        return {"results": []}

    results = []
    search_dirs = [base / name] if name else [d for d in base.iterdir() if d.is_dir()]

    for config_dir in search_dirs:
        if not config_dir.exists():
            continue
        for analysis_file in config_dir.glob("**/analysis.json"):
            result_dir = analysis_file.parent
            try:
                with open(analysis_file, "r", encoding="utf-8") as f:
                    analysis = json.load(f)
                config_file = result_dir / "config.json"
                config_data = {}
                if config_file.exists():
                    with open(config_file, "r", encoding="utf-8") as f:
                        config_data = json.load(f)

                bt = config_data.get("backtest", {})
                bot = config_data.get("bot", {})

                # Support old & new analysis key formats
                adg = analysis.get("adg_usd", analysis.get("adg", 0))
                drawdown = analysis.get("drawdown_worst_usd", analysis.get("drawdown_worst", 0))
                sharpe = analysis.get("sharpe_ratio_usd", analysis.get("sharpe_ratio", 0))
                eqbal_diff = analysis.get(
                    "equity_balance_diff_neg_max_usd",
                    analysis.get("equity_balance_diff_neg_max", 0)
                )
                gain = analysis.get("gain_usd", analysis.get("gain", 0))
                starting_balance = bt.get("starting_balance", 0)
                final_balance = starting_balance * gain if starting_balance else 0

                # Liquidation detection: use passivbot's flag if available,
                # fall back to heuristic for older results
                liq_threshold = bt.get("liquidation_threshold", 0.05)
                if "liquidated" in analysis:
                    liquidated = bool(analysis["liquidated"])
                else:
                    liquidated = (
                        drawdown >= 0.95
                        or eqbal_diff >= 0.95
                        or (starting_balance > 0 and final_balance < starting_balance * liq_threshold)
                    )

                results.append({
                    "path": str(result_dir),
                    "display_name": str(result_dir.relative_to(base)),
                    "config_name": config_dir.name,
                    "result_name": result_dir.name,
                    "exchange_dir": result_dir.parent.name,
                    "adg": adg,
                    "drawdown_worst": drawdown,
                    "sharpe_ratio": sharpe,
                    "equity_balance_diff_neg_max": eqbal_diff,
                    "gain": gain,
                    "starting_balance": starting_balance,
                    "final_balance": final_balance,
                    "liquidated": liquidated,
                    "exchanges": bt.get("exchanges", []),
                    "start_date": bt.get("start_date", ""),
                    "end_date": bt.get("end_date", ""),
                    "btc_collateral_cap": float(bt.get("btc_collateral_cap") or 0),
                    "twe_long": bot.get("long", {}).get("total_wallet_exposure_limit", 0),
                    "twe_short": bot.get("short", {}).get("total_wallet_exposure_limit", 0),
                    "pos_long": bot.get("long", {}).get("n_positions", 0),
                    "pos_short": bot.get("short", {}).get("n_positions", 0),
                    "modified": datetime.datetime.fromtimestamp(
                        analysis_file.stat().st_mtime
                    ).isoformat(),
                    "analysis": analysis,
                })
            except Exception as e:
                _log(SERVICE, f"Error reading result {result_dir}: {e}", level="WARNING")

    return {"results": results}


@router.get("/legacy/results")
def list_legacy_results(session: SessionToken = Depends(require_auth)):
    """List legacy results found under pb7/backtests/* outside pbgui."""
    root = _bt_results_root().resolve()
    if not root.exists():
        return {"results": []}

    results = []
    for source_dir in _legacy_results_roots():
        source_name = source_dir.name
        for analysis_file in source_dir.glob("**/analysis.json"):
            result_dir = analysis_file.parent
            try:
                with open(analysis_file, "r", encoding="utf-8") as f:
                    analysis = json.load(f)
                config_file = result_dir / "config.json"
                config_data = {}
                if config_file.exists():
                    with open(config_file, "r", encoding="utf-8") as f:
                        config_data = json.load(f)

                bt = config_data.get("backtest", {})
                bot = config_data.get("bot", {})

                adg = analysis.get("adg_usd", analysis.get("adg", 0))
                drawdown = analysis.get("drawdown_worst_usd", analysis.get("drawdown_worst", 0))
                sharpe = analysis.get("sharpe_ratio_usd", analysis.get("sharpe_ratio", 0))
                eqbal_diff = analysis.get(
                    "equity_balance_diff_neg_max_usd",
                    analysis.get("equity_balance_diff_neg_max", 0)
                )
                gain = analysis.get("gain_usd", analysis.get("gain", 0))
                starting_balance = bt.get("starting_balance", 0)
                final_balance = starting_balance * gain if starting_balance else 0

                liq_threshold = bt.get("liquidation_threshold", 0.05)
                if "liquidated" in analysis:
                    liquidated = bool(analysis["liquidated"])
                else:
                    liquidated = (
                        drawdown >= 0.95
                        or eqbal_diff >= 0.95
                        or (starting_balance > 0 and final_balance < starting_balance * liq_threshold)
                    )

                base_dir_val = str(bt.get("base_dir") or "").strip()
                base_dir_name = Path(base_dir_val).name if base_dir_val else ""
                if base_dir_name and base_dir_name != "backtests":
                    config_name = base_dir_name
                    suggested_name = base_dir_name
                else:
                    config_name = f"Legacy {source_name}"
                    suggested_name = f"legacy_{source_name}_{result_dir.name}"

                results.append({
                    "path": str(result_dir),
                    "display_name": str(result_dir.relative_to(root)),
                    "config_name": config_name,
                    "result_name": result_dir.name,
                    "exchange_dir": source_name,
                    "suggested_name": suggested_name,
                    "adg": adg,
                    "drawdown_worst": drawdown,
                    "sharpe_ratio": sharpe,
                    "equity_balance_diff_neg_max": eqbal_diff,
                    "gain": gain,
                    "starting_balance": starting_balance,
                    "final_balance": final_balance,
                    "liquidated": liquidated,
                    "exchanges": bt.get("exchanges", []),
                    "start_date": bt.get("start_date", ""),
                    "end_date": bt.get("end_date", ""),
                    "btc_collateral_cap": float(bt.get("btc_collateral_cap") or 0),
                    "twe_long": bot.get("long", {}).get("total_wallet_exposure_limit", 0),
                    "twe_short": bot.get("short", {}).get("total_wallet_exposure_limit", 0),
                    "pos_long": bot.get("long", {}).get("n_positions", 0),
                    "pos_short": bot.get("short", {}).get("n_positions", 0),
                    "modified": datetime.datetime.fromtimestamp(
                        analysis_file.stat().st_mtime
                    ).isoformat(),
                    "analysis": analysis,
                })
            except Exception as e:
                _log(SERVICE, f"Error reading legacy result {result_dir}: {e}", level="WARNING")

    return {"results": results}


@router.get("/results/analysis")
def get_result_analysis(path: str, session: SessionToken = Depends(require_auth)):
    """Get full analysis.json for a result. Path is the result directory."""
    result_dir = _resolve_result_dir(path)
    analysis_file = result_dir / "analysis.json"
    if not analysis_file.exists():
        raise HTTPException(404, "analysis.json not found")
    with open(analysis_file, "r", encoding="utf-8") as f:
        return json.load(f)


@router.get("/results/config")
def get_result_config(path: str, session: SessionToken = Depends(require_auth)):
    """Get config.json for a result, with missing params neutralized."""
    result_dir = _resolve_result_dir(path)
    config_file = result_dir / "config.json"
    if not config_file.exists():
        raise HTTPException(404, "config.json not found")
    return load_pb7_config(config_file, neutralize_added=True)


@router.get("/results/equity")
def get_result_equity(path: str, session: SessionToken = Depends(require_auth)):
    """Stream balance_and_equity CSV file directly for client-side parsing."""
    result_dir = _resolve_result_dir(path)

    csv_file = result_dir / "balance_and_equity.csv"
    gz_file = result_dir / "balance_and_equity.csv.gz"

    if csv_file.exists():
        return FileResponse(str(csv_file), media_type="text/csv",
                            headers={"Cache-Control": "max-age=3600"})
    elif gz_file.exists():
        return FileResponse(str(gz_file), media_type="text/csv",
                            headers={"Content-Encoding": "gzip",
                                     "Cache-Control": "max-age=3600"})
    else:
        raise HTTPException(404, "balance_and_equity data not found")


@router.get("/results/fills")
def get_result_fills(path: str, session: SessionToken = Depends(require_auth)):
    """Stream fills CSV file directly for client-side parsing."""
    result_dir = _resolve_result_dir(path)

    csv_file = result_dir / "fills.csv"
    gz_file = result_dir / "fills.csv.gz"

    if csv_file.exists():
        return FileResponse(str(csv_file), media_type="text/csv",
                            headers={"Cache-Control": "max-age=3600"})
    elif gz_file.exists():
        return FileResponse(str(gz_file), media_type="text/csv",
                            headers={"Content-Encoding": "gzip",
                                     "Cache-Control": "max-age=3600"})
    else:
        raise HTTPException(404, "fills data not found")


@router.get("/results/files")
def list_result_files(path: str, session: SessionToken = Depends(require_auth)):
    """List all files in a result directory (for UI to know what's available)."""
    result_dir = _resolve_result_dir(path)
    if not result_dir.exists():
        raise HTTPException(404, "Result not found")
    files = []
    for f in sorted(result_dir.rglob("*")):
        if f.is_file():
            files.append(str(f.relative_to(result_dir)))
    return {"files": files}


@router.get("/results/image")
def get_result_image(path: str, filename: str,
                     session: SessionToken = Depends(require_auth)):
    """Serve a PNG image from a result directory."""
    result_dir = _resolve_result_dir(path)
    # Security: prevent path traversal
    if ".." in filename:
        raise HTTPException(400, "Invalid filename")
    img_path = (result_dir / filename).resolve()
    if not img_path.is_relative_to(result_dir.resolve()):
        raise HTTPException(400, "Invalid filename")
    if not img_path.exists() or not img_path.is_file():
        raise HTTPException(404, "Image not found")
    media = "image/png" if img_path.suffix == ".png" else "application/octet-stream"
    return FileResponse(str(img_path), media_type=media,
                        headers={"Cache-Control": "max-age=3600"})


@router.delete("/results")
def delete_result(path: str, session: SessionToken = Depends(require_auth)):
    """Delete a single result directory."""
    result_dir = _resolve_result_dir(path, allow_legacy=False, allow_archives=False)
    if not result_dir.exists():
        raise HTTPException(404, "Result not found")
    rmtree(str(result_dir), ignore_errors=True)
    return {"ok": True}


@router.delete("/legacy/results")
def delete_legacy_result(path: str, session: SessionToken = Depends(require_auth)):
    """Delete a single legacy result directory."""
    result_dir = _resolve_result_dir(path, allow_pbgui=False, allow_legacy=True, allow_archives=False)
    if not result_dir.exists():
        raise HTTPException(404, "Result not found")
    legacy_root = _find_legacy_result_root(result_dir)
    rmtree(str(result_dir), ignore_errors=True)
    parent = result_dir.parent
    while parent != legacy_root and parent.is_dir():
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent
    return {"ok": True}


# ── REST: Archives ────────────────────────────────────────────

@router.get("/archives")
def list_archives(session: SessionToken = Depends(require_auth)):
    """List configured git archives."""
    base = _archives_dir()
    archives = []
    if base.exists():
        for d in sorted(base.iterdir()):
            git_config = d / ".git" / "config"
            if git_config.exists():
                # Parse remote URL
                url = ""
                try:
                    cfg = configparser.ConfigParser()
                    cfg.read(str(git_config))
                    url = cfg.get('remote "origin"', "url", fallback="")
                except Exception:
                    pass
                # Count configs
                config_count = len(list(d.glob("**/analysis.json")))
                archives.append({
                    "name": d.name,
                    "path": str(d),
                    "url": url,
                    "configs": config_count,
                })
    return {"archives": archives}


@router.get("/archives/{name}/results")
def list_archive_results(name: str, session: SessionToken = Depends(require_auth)):
    """List results in an archive (same format as /results)."""
    _validate_name(name)
    archive_dir = _archives_dir() / name
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")

    results = []
    for analysis_file in archive_dir.glob("**/analysis.json"):
        result_dir = analysis_file.parent
        try:
            with open(analysis_file, "r", encoding="utf-8") as f:
                analysis = json.load(f)
            config_file = result_dir / "config.json"
            config_data = {}
            if config_file.exists():
                with open(config_file, "r", encoding="utf-8") as f:
                    config_data = json.load(f)

            bt = config_data.get("backtest", {})
            bot = config_data.get("bot", {})
            adg = analysis.get("adg_usd", analysis.get("adg", 0))
            drawdown = analysis.get("drawdown_worst_usd", analysis.get("drawdown_worst", 0))
            sharpe = analysis.get("sharpe_ratio_usd", analysis.get("sharpe_ratio", 0))
            eqbal_diff = analysis.get(
                "equity_balance_diff_neg_max_usd",
                analysis.get("equity_balance_diff_neg_max", 0)
            )
            gain = analysis.get("gain_usd", analysis.get("gain", 0))
            starting_balance = bt.get("starting_balance", 0)
            final_balance = starting_balance * gain if starting_balance else 0
            liq_threshold = bt.get("liquidation_threshold", 0.05)
            if "liquidated" in analysis:
                liquidated = bool(analysis["liquidated"])
            else:
                liquidated = (
                    drawdown >= 0.95
                    or eqbal_diff >= 0.95
                    or (starting_balance > 0 and final_balance < starting_balance * liq_threshold)
                )

            # config_name: last part of backtest.base_dir (e.g. "RENDER_adg_sharpe_omega...")
            # Falls back to directory-based heuristic if base_dir is missing.
            base_dir_val = bt.get("base_dir", "")
            if base_dir_val:
                arc_config_name = Path(base_dir_val).name
            else:
                rel_parts = result_dir.relative_to(archive_dir).parts
                if len(rel_parts) >= 3:
                    arc_config_name = rel_parts[-3]  # part before exchange/timestamp
                elif len(rel_parts) >= 2:
                    arc_config_name = rel_parts[0]
                else:
                    arc_config_name = result_dir.parent.name

            results.append({
                "path": str(result_dir),
                "display_name": str(result_dir.relative_to(archive_dir)),
                "config_name": arc_config_name,
                "result_name": result_dir.name,
                "adg": adg,
                "drawdown_worst": drawdown,
                "sharpe_ratio": sharpe,
                "equity_balance_diff_neg_max": eqbal_diff,
                "gain": gain,
                "starting_balance": starting_balance,
                "final_balance": final_balance,
                "liquidated": liquidated,
                "exchanges": bt.get("exchanges", []),
                "start_date": bt.get("start_date", ""),
                "end_date": bt.get("end_date", ""),
                "btc_collateral_cap": float(bt.get("btc_collateral_cap") or 0),
                "twe_long": bot.get("long", {}).get("total_wallet_exposure_limit", 0),
                "twe_short": bot.get("short", {}).get("total_wallet_exposure_limit", 0),
                "pos_long": bot.get("long", {}).get("n_positions", 0),
                "pos_short": bot.get("short", {}).get("n_positions", 0),
                "modified": datetime.datetime.fromtimestamp(
                    analysis_file.stat().st_mtime
                ).isoformat(),
                "analysis": analysis,
            })
        except Exception as e:
            _log(SERVICE, f"Error reading archive result {result_dir}: {e}", level="WARNING")

    return {"results": results}


@router.post("/archives")
def create_archive(body: dict, session: SessionToken = Depends(require_auth)):
    """Clone a git repo as archive."""
    name = body.get("name", "")
    url = body.get("url", "")
    if not name or not url:
        raise HTTPException(400, "name and url are required")
    _validate_name(name)

    dest = _archives_dir() / name
    if dest.exists():
        raise HTTPException(409, f"Archive '{name}' already exists")

    _archives_dir().mkdir(parents=True, exist_ok=True)
    try:
        result = subprocess.run(
            ["git", "clone", url, str(dest)],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0:
            raise HTTPException(500, f"git clone failed: {result.stderr}")
    except subprocess.TimeoutExpired:
        raise HTTPException(500, "git clone timed out")

    return {"ok": True, "name": name}


@router.delete("/archives/{name}")
def delete_archive(name: str, session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    dest = _archives_dir() / name
    if not dest.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    rmtree(str(dest), ignore_errors=True)
    return {"ok": True}


@router.delete("/archives/{name}/results")
def delete_archive_result(name: str, path: str, session: SessionToken = Depends(require_auth)):
    """Delete a single result directory from an archive."""
    _validate_name(name)
    result_dir = Path(path).resolve()
    archive_base = (_archives_dir() / name).resolve()
    if not result_dir.is_relative_to(archive_base):
        raise HTTPException(400, "Invalid result path")
    if not result_dir.exists():
        raise HTTPException(404, "Result not found")
    rmtree(str(result_dir), ignore_errors=True)
    # Remove empty parent directories up to (but not including) the archive root
    parent = result_dir.parent
    while parent != archive_base and parent.is_dir():
        try:
            parent.rmdir()  # only succeeds if directory is empty
        except OSError:
            break  # not empty — stop climbing
        parent = parent.parent
    return {"ok": True}
def git_pull(name: str, session: SessionToken = Depends(require_auth)):
    """Pull a single archive."""
    _validate_name(name)
    dest = _archives_dir() / name
    if not dest.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    try:
        result = subprocess.run(
            ["git", "pull"], cwd=str(dest),
            capture_output=True, text=True, timeout=60
        )
        output = (result.stdout + result.stderr).strip()
        _log_archive(f"[{name}] git pull: {output or 'ok'}")
        return {"ok": True, "output": output}
    except subprocess.TimeoutExpired:
        raise HTTPException(500, "git pull timed out")


@router.post("/archives/pull-all")
def pull_all_archives(session: SessionToken = Depends(require_auth)):
    """Pull all cloned archives (like Streamlit git_pull)."""
    _log_archive("Manual pull-all triggered")
    results = _pull_all_archives_sync()
    return {"ok": True, "results": results}


@router.post("/archives/{name}/push")
def git_push(name: str, body: dict = None, session: SessionToken = Depends(require_auth)):
    """Git pull + add + commit + push for own archive.
    Accepts optional access_token to inject into the HTTPS remote URL for auth.
    Pass dry_run=true to test credentials without actually pushing.
    """
    _validate_name(name)
    dest = _archives_dir() / name
    if not dest.exists():
        raise HTTPException(404, f"Archive '{name}' not found")

    body = body or {}
    username = body.get("username", "")
    email = body.get("email", "")
    access_token = body.get("access_token", "")
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = body.get("message", f"Update {name} at {timestamp}")
    dry_run = bool(body.get("dry_run", False))

    log_lines = []

    try:
        if username:
            subprocess.run(["git", "config", "user.name", username], cwd=str(dest),
                           capture_output=True, timeout=10)
        if email:
            subprocess.run(["git", "config", "user.email", email], cwd=str(dest),
                           capture_output=True, timeout=10)

        if not dry_run:
            # Pull before push (like Streamlit)
            pull_result = subprocess.run(
                ["git", "pull"], cwd=str(dest),
                capture_output=True, text=True, timeout=60
            )
            pull_out = (pull_result.stdout + pull_result.stderr).strip()
            _log_archive(f"[{name}] git pull (pre-push): {pull_out or 'ok'}")
            log_lines.append(f"Git pull:\n{pull_out}")
            if pull_result.returncode != 0:
                raise HTTPException(500, f"git pull failed: {pull_result.stderr}")

            add_result = subprocess.run(["git", "add", "-A"], cwd=str(dest),
                                        capture_output=True, text=True, timeout=30)
            log_lines.append(f"Git add:\n{(add_result.stdout + add_result.stderr).strip()}")

            commit_result = subprocess.run(
                ["git", "commit", "-m", message], cwd=str(dest),
                capture_output=True, text=True, timeout=30
            )
            commit_out = (commit_result.stdout + commit_result.stderr).strip()
            _log_archive(f"[{name}] git commit: {commit_out}")
            log_lines.append(f"Git commit:\n{commit_out}")

        # Build push command — inject access token into HTTPS URL when provided
        push_url = None
        if access_token:
            url_result = subprocess.run(
                ["git", "remote", "get-url", "origin"], cwd=str(dest),
                capture_output=True, text=True, timeout=10
            )
            remote_url = url_result.stdout.strip()
            if remote_url.startswith("http://"):
                push_url = remote_url.replace("http://", f"http://{access_token}@", 1)
            elif remote_url.startswith("https://"):
                push_url = remote_url.replace("https://", f"https://{access_token}@", 1)

        push_cmd = ["git", "push"]
        if dry_run:
            push_cmd.append("--dry-run")
        if push_url:
            push_cmd.append(push_url)

        result = subprocess.run(
            push_cmd, cwd=str(dest),
            capture_output=True, text=True, timeout=120
        )
        push_out = (result.stdout + result.stderr).strip()
        _log_archive(f"[{name}] git push{'(dry-run)' if dry_run else ''}: {push_out}")
        log_lines.append(f"Git push:\n{push_out}")

        if result.returncode != 0:
            raise HTTPException(500, f"git push failed: {result.stderr}")
        return {"ok": True, "output": "\n\n".join(log_lines)}
    except HTTPException:
        raise
    except subprocess.TimeoutExpired:
        raise HTTPException(500, "git operation timed out")


@router.post("/archives/{name}/add-config")
def add_config_to_archive(name: str, body: dict,
                          session: SessionToken = Depends(require_auth)):
    """Copy a result directory into an archive."""
    _validate_name(name)
    source_path = body.get("source_path", "")
    dest_name = body.get("dest_name", "")
    if not source_path or not dest_name:
        raise HTTPException(400, "source_path and dest_name are required")

    # Compute dest_name the same way Streamlit does: everything after the last /pbgui/ segment
    if not dest_name:
        parts = source_path.replace("\\", "/").split("/pbgui/")
        dest_name = parts[-1].strip("/") if len(parts) > 1 else Path(source_path).name

    src = Path(source_path)
    if not src.exists():
        raise HTTPException(404, "Source path not found")

    # Security: validate source is under current results, legacy results, or archives
    src = _resolve_result_dir(src)

    archive_dir = _archives_dir() / name
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")

    # Read archive settings from ini to get my_archive_path (same section as Streamlit)
    my_path = load_ini("config_archive", "my_archive_path") or ""
    dest_dir = archive_dir / my_path / dest_name if my_path else archive_dir / dest_name
    dest_dir.parent.mkdir(parents=True, exist_ok=True)

    if dest_dir.exists():
        raise HTTPException(409, f"Destination '{dest_name}' already exists in archive")

    copytree(str(src), str(dest_dir))
    return {"ok": True}


@router.get("/archives/settings")
def get_archive_settings(session: SessionToken = Depends(require_auth)):
    """Get archive configuration from INI (same section/keys as Streamlit BacktestV7.py)."""
    section = "config_archive"
    return {
        "my_archive":        load_ini(section, "my_archive") or "",
        "my_archive_path":   load_ini(section, "my_archive_path") or "",
        "username":          load_ini(section, "my_archive_username") or "",
        "email":             load_ini(section, "my_archive_email") or "",
        "access_token":      load_ini(section, "my_archive_access_token") or "",
        "auto_pull_interval": _read_auto_pull_interval(),
    }


@router.post("/archives/settings")
def save_archive_settings(body: dict, session: SessionToken = Depends(require_auth)):
    """Save archive configuration to INI (same section/keys as Streamlit BacktestV7.py)."""
    section = "config_archive"
    mapping = {
        "my_archive":      "my_archive",
        "my_archive_path": "my_archive_path",
        "username":        "my_archive_username",
        "email":            "my_archive_email",
        "access_token":    "my_archive_access_token",
    }
    for body_key, ini_key in mapping.items():
        if body_key in body:
            save_ini(section, ini_key, str(body[body_key]))
    if "auto_pull_interval" in body:
        try:
            interval = max(0, int(body["auto_pull_interval"]))
        except (ValueError, TypeError):
            interval = 0
        save_ini(section, "auto_pull_interval", str(interval))
    return {"ok": True}


