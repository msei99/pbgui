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
import subprocess
import traceback
import uuid
from pathlib import Path, PurePath
from shutil import copytree, rmtree
from typing import Optional

import psutil
from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse

from api.auth import SessionToken, require_auth, validate_token
from Config import ConfigV7
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR, load_ini, save_ini, pb7dir, pb7venv

SERVICE = "BacktestV7API"

router = APIRouter()

# ── Helpers ───────────────────────────────────────────────────

def _validate_name(name: str):
    """Reject path-traversal attempts."""
    if not name or any(c in name for c in ("/", "\\", "\x00")) or name in (".", ".."):
        raise HTTPException(400, "Invalid name")


def _bt_queue_dir() -> Path:
    return Path(PBGDIR) / "data" / "bt_v7_queue"


def _bt_configs_dir() -> Path:
    return Path(PBGDIR) / "data" / "bt_v7"


def _bt_results_base() -> str:
    """Base directory for backtest results (inside pb7)."""
    return str(Path(pb7dir()) / "backtests" / "pbgui")


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


# ── WebSocket ─────────────────────────────────────────────────

_ws_clients: set[WebSocket] = set()


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


# ── Lifespan hook ─────────────────────────────────────────────

def startup():
    """Called from PBApiServer lifespan to start the worker."""
    _worker.start()


def shutdown():
    """Called from PBApiServer lifespan to stop the worker."""
    _worker.stop()


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


# ── REST: PBGui data path ─────────────────────────────────────

@router.get("/pbgui_data_path")
def get_pbgui_data_path(session: SessionToken = Depends(require_auth)):
    """Return the PBGui-managed market data root directory."""
    from market_data import get_market_data_root_dir
    return {"path": str(get_market_data_root_dir())}


# ── REST: Settings ────────────────────────────────────────────

@router.get("/settings")
def get_settings(session: SessionToken = Depends(require_auth)):
    settings = _read_ini_section()
    cpu_max = multiprocessing.cpu_count()
    return {
        "autostart": settings.get("autostart", "False").lower() == "true",
        "cpu": min(int(settings.get("cpu", "1")), cpu_max),
        "cpu_max": cpu_max,
    }


@router.post("/settings")
def update_settings(body: dict, session: SessionToken = Depends(require_auth)):
    if "autostart" in body:
        _write_ini("autostart", str(bool(body["autostart"])))
    if "cpu" in body:
        cpu = max(1, min(int(body["cpu"]), multiprocessing.cpu_count()))
        _write_ini("cpu", str(cpu))
    _store.notify()
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
    cfg = ConfigV7(str(cfg_file))
    cfg.load_config()
    return cfg.config


@router.put("/configs/{name}")
def save_config(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    cfg_dir = _bt_configs_dir() / name
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_file = cfg_dir / "backtest.json"
    cfg = ConfigV7(str(cfg_file))
    if cfg_file.exists():
        cfg.load_config()
    cfg.config = body
    cfg.save_config()
    return {"ok": True, "name": name}


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
    src = _bt_configs_dir() / name / "backtest.json"
    if not src.exists():
        raise HTTPException(404, f"Config '{name}' not found")
    dst_dir = _bt_configs_dir() / new_name
    if dst_dir.exists():
        raise HTTPException(409, f"Config '{new_name}' already exists")
    dst_dir.mkdir(parents=True, exist_ok=True)
    import shutil
    shutil.copy2(str(src), str(dst_dir / "backtest.json"))
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

    Body: {name, config} where config is the full backtest JSON.
    The config is saved to pb7/configs/backtest/ and queue metadata
    is saved to data/bt_v7_queue/.
    """
    name = body.get("name", "")
    config = body.get("config")
    if not name or not config:
        raise HTTPException(400, "name and config are required")

    filename = str(uuid.uuid4())
    bt = config.get("backtest", {})
    exchanges = bt.get("exchanges", [])
    exchange_str = exchanges if isinstance(exchanges, list) else [exchanges]

    # Save config to pb7/configs/backtest/
    config_dir = Path(pb7dir()) / "configs" / "backtest"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_dir / f"{filename}.json"
    with open(config_file, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)

    # Save queue metadata
    queue_dir = _bt_queue_dir()
    queue_dir.mkdir(parents=True, exist_ok=True)
    queue_file = queue_dir / f"{filename}.json"
    queue_data = {
        "name": name,
        "filename": filename,
        "json": str(config_file),
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

                results.append({
                    "path": str(result_dir),
                    "config_name": config_dir.name,
                    "result_name": result_dir.name,
                    "exchange_dir": result_dir.parent.name,
                    "adg": adg,
                    "drawdown_worst": drawdown,
                    "sharpe_ratio": sharpe,
                    "equity_balance_diff_neg_max": eqbal_diff,
                    "gain": gain,
                    "starting_balance": starting_balance,
                    "final_balance": starting_balance * gain if starting_balance else 0,
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


@router.get("/results/analysis")
def get_result_analysis(path: str, session: SessionToken = Depends(require_auth)):
    """Get full analysis.json for a result. Path is the result directory."""
    result_dir = Path(path)
    # Security: ensure path is under results base
    base = Path(_bt_results_base()).resolve()
    if not result_dir.resolve().is_relative_to(base):
        # Also allow archive paths
        archives = _archives_dir().resolve()
        if not result_dir.resolve().is_relative_to(archives):
            raise HTTPException(400, "Invalid result path")
    analysis_file = result_dir / "analysis.json"
    if not analysis_file.exists():
        raise HTTPException(404, "analysis.json not found")
    with open(analysis_file, "r", encoding="utf-8") as f:
        return json.load(f)


@router.get("/results/config")
def get_result_config(path: str, session: SessionToken = Depends(require_auth)):
    """Get config.json for a result."""
    result_dir = Path(path)
    base = Path(_bt_results_base()).resolve()
    if not result_dir.resolve().is_relative_to(base):
        archives = _archives_dir().resolve()
        if not result_dir.resolve().is_relative_to(archives):
            raise HTTPException(400, "Invalid result path")
    config_file = result_dir / "config.json"
    if not config_file.exists():
        raise HTTPException(404, "config.json not found")
    with open(config_file, "r", encoding="utf-8") as f:
        return json.load(f)


@router.get("/results/equity")
def get_result_equity(path: str, session: SessionToken = Depends(require_auth)):
    """Stream balance_and_equity CSV file directly for client-side parsing."""
    result_dir = Path(path)
    base = Path(_bt_results_base()).resolve()
    if not result_dir.resolve().is_relative_to(base):
        archives = _archives_dir().resolve()
        if not result_dir.resolve().is_relative_to(archives):
            raise HTTPException(400, "Invalid result path")

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
    result_dir = Path(path)
    base = Path(_bt_results_base()).resolve()
    if not result_dir.resolve().is_relative_to(base):
        archives = _archives_dir().resolve()
        if not result_dir.resolve().is_relative_to(archives):
            raise HTTPException(400, "Invalid result path")

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
    result_dir = Path(path)
    base = Path(_bt_results_base()).resolve()
    if not result_dir.resolve().is_relative_to(base):
        archives = _archives_dir().resolve()
        if not result_dir.resolve().is_relative_to(archives):
            raise HTTPException(400, "Invalid result path")
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
    result_dir = Path(path)
    base = Path(_bt_results_base()).resolve()
    if not result_dir.resolve().is_relative_to(base):
        archives = _archives_dir().resolve()
        if not result_dir.resolve().is_relative_to(archives):
            raise HTTPException(400, "Invalid result path")
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
    result_dir = Path(path)
    base = Path(_bt_results_base()).resolve()
    if not result_dir.resolve().is_relative_to(base):
        raise HTTPException(400, "Invalid result path")
    if not result_dir.exists():
        raise HTTPException(404, "Result not found")
    rmtree(str(result_dir), ignore_errors=True)
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
            gain = analysis.get("gain_usd", analysis.get("gain", 0))
            starting_balance = bt.get("starting_balance", 0)

            results.append({
                "path": str(result_dir),
                "config_name": result_dir.parent.name,
                "result_name": result_dir.name,
                "adg": adg,
                "gain": gain,
                "starting_balance": starting_balance,
                "final_balance": starting_balance * (1 + gain) if starting_balance else 0,
                "exchanges": bt.get("exchanges", []),
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


@router.post("/archives/{name}/pull")
def git_pull(name: str, session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    dest = _archives_dir() / name
    if not dest.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    try:
        result = subprocess.run(
            ["git", "pull"], cwd=str(dest),
            capture_output=True, text=True, timeout=60
        )
        return {"ok": True, "output": result.stdout}
    except subprocess.TimeoutExpired:
        raise HTTPException(500, "git pull timed out")


@router.post("/archives/{name}/push")
def git_push(name: str, body: dict = None, session: SessionToken = Depends(require_auth)):
    """Git add + commit + push for an archive."""
    _validate_name(name)
    dest = _archives_dir() / name
    if not dest.exists():
        raise HTTPException(404, f"Archive '{name}' not found")

    body = body or {}
    username = body.get("username", "")
    email = body.get("email", "")
    message = body.get("message", "Update via PBGui")

    try:
        if username:
            subprocess.run(["git", "config", "user.name", username], cwd=str(dest),
                           capture_output=True, timeout=10)
        if email:
            subprocess.run(["git", "config", "user.email", email], cwd=str(dest),
                           capture_output=True, timeout=10)

        subprocess.run(["git", "add", "-A"], cwd=str(dest), capture_output=True, timeout=30)
        subprocess.run(
            ["git", "commit", "-m", message], cwd=str(dest),
            capture_output=True, text=True, timeout=30
        )
        result = subprocess.run(
            ["git", "push"], cwd=str(dest),
            capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0:
            raise HTTPException(500, f"git push failed: {result.stderr}")
        return {"ok": True, "output": result.stdout}
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

    src = Path(source_path)
    if not src.exists():
        raise HTTPException(404, "Source path not found")

    # Security: validate source is under results base or archives
    base = Path(_bt_results_base()).resolve()
    archives = _archives_dir().resolve()
    if not (src.resolve().is_relative_to(base) or src.resolve().is_relative_to(archives)):
        raise HTTPException(400, "Invalid source path")

    archive_dir = _archives_dir() / name
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")

    # Read archive settings from ini to get my_archive_path
    my_path = load_ini("config_v7_archives", "my_archive_path") or ""
    dest_dir = archive_dir / my_path / dest_name if my_path else archive_dir / dest_name
    dest_dir.parent.mkdir(parents=True, exist_ok=True)

    if dest_dir.exists():
        raise HTTPException(409, f"Destination '{dest_name}' already exists in archive")

    copytree(str(src), str(dest_dir))
    return {"ok": True}


@router.get("/archives/settings")
def get_archive_settings(session: SessionToken = Depends(require_auth)):
    """Get archive configuration from INI."""
    section = "config_v7_archives"
    return {
        "my_archive": load_ini(section, "my_archive") or "",
        "my_archive_path": load_ini(section, "my_archive_path") or "",
        "username": load_ini(section, "username") or "",
        "email": load_ini(section, "email") or "",
        "access_token": load_ini(section, "access_token") or "",
    }


@router.post("/archives/settings")
def save_archive_settings(body: dict, session: SessionToken = Depends(require_auth)):
    """Save archive configuration to INI."""
    section = "config_v7_archives"
    for key in ("my_archive", "my_archive_path", "username", "email", "access_token"):
        if key in body:
            save_ini(section, key, str(body[key]))
    return {"ok": True}
