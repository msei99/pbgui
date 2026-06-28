from __future__ import annotations

import json
import os
import select
import signal
import shlex
import shutil
import subprocess
import sys
import time
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from hyperliquid_aws import (
    download_hyperliquid_l2book_aws,
    get_hyperliquid_archive_day_range_aws,
)
from hyperliquid_best_1m import improve_best_hyperliquid_1m_archive_for_coin, _is_stock_perp_coin
from binance_best_1m import improve_best_binance_1m_for_coin
from bybit_best_1m import improve_best_bybit_1m_for_coin
from okx_best_1m import (
    REST_RATE_PER_SECOND as _OKX_REST_RATE_PER_SECOND,
    RateLimiter as _OkxRateLimiter,
    improve_best_okx_1m_for_coin,
    get_storage_coin_dir as _okx_storage_coin_dir,
)
from market_data import (
    append_exchange_download_log,
    get_market_data_root_dir,
    load_aws_profile_credentials,
    load_aws_profile_region,
    normalize_market_data_coin_dir,
)
from market_data_sources import get_source_codes_for_day, get_oldest_day_with_source_code, SOURCE_CODE_L2BOOK
from pbgui_purefunc import load_ini
from task_queue import (
    clear_worker_pid,
    ensure_task_dirs,
    get_task_state_dir,
    get_job_log_path,
    is_pid_running,
    move_job_file,
    update_job_file,
    write_worker_pid,
    enqueue_job,
)
from inventory_cache import refresh_coin as _refresh_inventory_coin, sweep_cache_mtimes as _sweep_cache_mtimes


_STOP = False
OKX_BEST_1M_PIPELINE_WORKERS = 2
OHLCV_COPY_EXCHANGES: dict[str, dict[str, str]] = {
    "binance": {"label": "Binance USDM", "storage": "binanceusdm"},
    "binanceusdm": {"label": "Binance USDM", "storage": "binanceusdm"},
    "bybit": {"label": "Bybit", "storage": "bybit"},
    "okx": {"label": "OKX", "storage": "okx"},
    "hyperliquid": {"label": "Hyperliquid", "storage": "hyperliquid"},
}
OHLCV_COPY_MODES = {"missing_only", "update"}
OHLCV_COPY_TARGET_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-@")
OHLCV_COPY_REMOTE_PATH_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/._-")
OHLCV_COPY_RSYNC_STAT_LABELS = (
    "Number of files",
    "Number of regular files transferred",
    "Total file size",
    "Total transferred file size",
    "Total bytes sent",
    "Total bytes received",
)


def _handle_stop(signum, frame):
    global _STOP
    _STOP = True


def _parse_day(day: str) -> datetime:
    return datetime.strptime(str(day).strip(), "%Y%m%d")


def _iter_day_chunks(start_day: str, end_day: str, chunk_days: int) -> list[tuple[str, str]]:
    d0 = _parse_day(start_day).date()
    d1 = _parse_day(end_day).date()
    if d1 < d0:
        raise ValueError("end_day must be >= start_day")
    cur = d0
    out: list[tuple[str, str]] = []
    while cur <= d1:
        ce = min(d1, cur + timedelta(days=int(chunk_days) - 1))
        out.append((cur.strftime("%Y%m%d"), ce.strftime("%Y%m%d")))
        cur = ce + timedelta(days=1)
    return out


def _iter_days(start_day: str, end_day: str) -> list[str]:
    d0 = _parse_day(start_day).date()
    d1 = _parse_day(end_day).date()
    if d1 < d0:
        raise ValueError("end_day must be >= start_day")
    out: list[str] = []
    cur = d0
    while cur <= d1:
        out.append(cur.strftime("%Y%m%d"))
        cur = cur + timedelta(days=1)
    return out


def _requeue_stale_running_jobs(max_age_s: int = 3600) -> None:
    running_dir = get_task_state_dir("running")
    pending_dir = get_task_state_dir("pending")
    now = int(time.time())
    if not running_dir.is_dir():
        return
    for p in sorted(running_dir.glob("*.json")):
        try:
            # Skip jobs whose owning worker process is still alive — requeueing
            # a job that another worker is actively running causes two workers to
            # process the same job concurrently (doubled log entries, data races).
            try:
                wpid = int(json.loads(p.read_text(encoding="utf-8")).get("worker_pid") or 0)
                if wpid > 0 and is_pid_running(wpid):
                    _job_log(f"skipping running job {p.name} — worker PID {wpid} still alive")
                    continue
            except Exception:
                pass
            st = p.stat()
            age = now - int(st.st_mtime)
            if age > int(max_age_s):
                update_job_file(p, mutate=lambda o: o.update({"status": "pending", "error": "requeued after worker restart"}))
                os.replace(p, pending_dir / p.name)
                _job_log(f"requeued interrupted job {p.name} (age={age}s)", level="WARNING")
        except Exception:
            continue


@dataclass
class JobContext:
    path: Path
    obj: dict[str, Any]


def _load_job(path: Path) -> dict[str, Any] | None:
    try:
        import json

        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _is_cancel_requested(job_path: Path) -> bool:
    obj = _load_job(job_path)
    if not obj:
        return False
    return bool(obj.get("cancel_requested"))


def _job_log(msg: str, level: str = "INFO") -> None:
    append_exchange_download_log("hyperliquid", f"[worker] {msg}", level=level)


def _append_to_job_log(job_id: str, msg: str) -> None:
    """Append one timestamped line to the per-job log file in _tasks/logs/."""
    try:
        p = get_job_log_path(job_id)
        p.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        with open(p, "a", encoding="utf-8") as f:
            f.write(f"{ts}  {msg}\n")
    except Exception:
        pass


def _init_job_log(job_id: str) -> None:
    """Rotate any existing per-job log before a fresh run begins.

    Renames the old file to ``<job_id>.prev.log`` so crash history is
    preserved without polluting the current run's log with stale entries.
    Without this, a requeued job would APPEND to the log from the previous
    (crashed) run, making the log appear doubled.
    """
    try:
        p = get_job_log_path(job_id)
        if p.exists():
            prev = p.with_name(f"{job_id}.prev.log")
            os.replace(p, prev)
    except Exception:
        pass


def _recent_corrupt_l2book_files(*, coin: str, since_ts: float) -> list[str]:
    coin_u = str(coin or "").strip().upper()
    if not coin_u:
        return []
    base = Path(__file__).resolve().parent / "data" / "ohlcv" / "hyperliquid" / "l2Book" / coin_u
    if not base.exists():
        return []
    out: list[str] = []
    try:
        for p in base.glob("*.corrupt.*"):
            try:
                if p.stat().st_mtime >= float(since_ts):
                    out.append(p.name)
            except Exception:
                continue
    except Exception:
        return []
    return sorted(out)


def _hours_missing_in_1m_src(*, coin: str, day: str) -> list[int]:
    """Return hour numbers (0..23) where 1m_src has no minute coverage."""

    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin)
    if not coin_dir:
        return list(range(24))

    codes = get_source_codes_for_day(exchange="hyperliquid", coin=coin_dir, day=day)
    if not isinstance(codes, list) or len(codes) < 1440:
        return list(range(24))

    out: list[int] = []
    for hour in range(24):
        h0 = hour * 60
        h1 = h0 + 60
        hour_codes = codes[h0:h1]
        if not any(int(c or 0) > 0 for c in hour_codes):
            out.append(hour)
    return out


def _run_job(job_path: Path) -> None:
    obj = _load_job(job_path)
    if not obj:
        move_job_file(job_path, "failed")
        return

    if bool(obj.get("cancel_requested")):
        update_job_file(job_path, mutate=lambda o: o.update({"status": "failed", "error": "cancelled", "run_requested": False, "run_requested_ts": 0}))
        move_job_file(job_path, "failed")
        return

    jtype = str(obj.get("type") or "").strip()
    payload = obj.get("payload")
    payload = payload if isinstance(payload, dict) else {}

    def mark_error(err: str) -> None:
        update_job_file(job_path, mutate=lambda o: o.update({"status": "failed", "error": str(err), "run_requested": False, "run_requested_ts": 0}))

    # Stamp this worker's PID so _requeue_stale_running_jobs on a concurrent
    # worker startup can check whether we are still alive before stealing the job.
    update_job_file(job_path, mutate=lambda o: o.update({"status": "running", "error": "", "worker_pid": os.getpid()}))

    try:
        if jtype == "hl_aws_l2book_auto":
            _run_hl_aws_l2book_auto(job_path, payload)
        elif jtype == "hl_best_1m":
            _run_hl_best_1m(job_path, payload)
        elif jtype == "binance_best_1m":
            _run_binance_best_1m(job_path, payload)
        elif jtype == "bybit_best_1m":
            _run_bybit_best_1m(job_path, payload)
        elif jtype == "okx_best_1m":
            _run_okx_best_1m(job_path, payload)
        elif jtype == "ohlcv_copy":
            _run_ohlcv_copy(job_path, payload)
        elif jtype == "ohlcv_copy_dry_run":
            _run_ohlcv_copy(job_path, payload, dry_run=True)
        else:
            raise RuntimeError(f"Unknown job type: {jtype}")

        update_job_file(job_path, mutate=lambda o: o.update({"status": "done", "run_requested": False, "run_requested_ts": 0}))
        move_job_file(job_path, "done")
    except Exception as e:
        _job_log(f"job error {job_path.name}: {e}")
        mark_error(str(e))
        move_job_file(job_path, "failed")


def start_pending_job(job_id: str) -> tuple[bool, str]:
    """Move one pending job to running and launch a detached one-shot runner.

    This path is used by the API `Run` action so it works immediately even when
    the long-running queue worker has not been restarted yet.
    """

    jid = str(job_id or "").strip()
    if not jid:
        return False, "Job ID is empty"

    pending_path = get_task_state_dir("pending") / f"{jid}.json"
    obj = _load_job(pending_path)
    if not obj:
        return False, "Job not found or not in pending state"
    if str(obj.get("status") or "").strip().lower() != "pending":
        return False, "Job not found or not in pending state"

    jtype = str(obj.get("type") or "").strip()
    if not jtype:
        return False, "Job type is missing"

    same_type_running = 0
    same_type_manual = 0
    running_dir = get_task_state_dir("running")
    for running_path in sorted(running_dir.glob("*.json")):
        running_obj = _load_job(running_path)
        if not running_obj:
            continue
        if str(running_obj.get("type") or "").strip() != jtype:
            continue
        same_type_running += 1
        if bool(running_obj.get("manual_parallel")):
            same_type_manual += 1

    if same_type_running >= 2 or same_type_manual >= 1:
        return False, f"Another manual {jtype} job is already running"

    try:
        update_job_file(
            pending_path,
            mutate=lambda o: o.update(
                {
                    "status": "pending",
                    "error": "",
                    "run_requested": False,
                    "run_requested_ts": 0,
                }
            ),
        )
        running_path = move_job_file(pending_path, "running")
        update_job_file(
            running_path,
            mutate=lambda o: o.update(
                {
                    "status": "running",
                    "error": "",
                    "manual_parallel": True,
                    "run_requested": False,
                    "run_requested_ts": 0,
                }
            ),
        )
    except Exception as exc:
        return False, f"Failed to prepare job start: {exc}"

    try:
        subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--run-job", str(running_path)],
            cwd=str(Path(__file__).resolve().parent),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        _job_log(f"started manual job {running_path.name} type={jtype} manual_parallel=1")
        return True, ""
    except Exception as exc:
        try:
            update_job_file(
                running_path,
                mutate=lambda o: o.update(
                    {
                        "status": "pending",
                        "manual_parallel": False,
                        "run_requested": False,
                        "run_requested_ts": 0,
                    }
                ),
            )
            move_job_file(running_path, "pending")
        except Exception:
            pass
        return False, f"Failed to launch manual runner: {exc}"


def run_single_job_file(job_path_str: str) -> int:
    """Run exactly one job file and exit.

    Used for the detached manual `Run` action from the API.
    """

    job_path = Path(str(job_path_str or "").strip())
    if not job_path.exists():
        return 1
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    _run_job(job_path)
    return 0


def _normalize_ohlcv_copy_exchange(exchange: Any) -> str:
    """Return the supported copy exchange key for a request value."""

    ex = str(exchange or "").strip().lower().replace("-", "")
    if ex == "binanceusdm":
        return "binance"
    return ex


def _normalize_ohlcv_copy_exchanges(raw_exchanges: Any) -> list[str]:
    """Validate and de-duplicate requested OHLCV copy exchanges."""

    if not isinstance(raw_exchanges, list):
        raw_exchanges = []
    exchanges: list[str] = []
    for raw_exchange in raw_exchanges:
        ex = _normalize_ohlcv_copy_exchange(raw_exchange)
        if not ex:
            continue
        if ex not in OHLCV_COPY_EXCHANGES:
            raise ValueError(f"Unsupported exchange for OHLCV copy: {raw_exchange}")
        if ex not in exchanges:
            exchanges.append(ex)
    if not exchanges:
        raise ValueError("No exchanges selected for OHLCV copy")
    return exchanges


def _normalize_ohlcv_copy_target(target: Any) -> str:
    """Validate the rsync remote target host field."""

    text = str(target or "").strip()
    if not text:
        raise ValueError("Remote target is required")
    if any(ch.isspace() for ch in text) or any(ch in text for ch in ("/", "\\", "\x00", ":")):
        raise ValueError("Remote target must be a host or user@host without spaces, slashes, or a path")
    if any(ch not in OHLCV_COPY_TARGET_CHARS for ch in text):
        raise ValueError("Remote target contains unsupported characters")
    if text in (".", ".."):
        raise ValueError("Remote target is invalid")
    return text


def _normalize_ohlcv_copy_destination_root(destination_root: Any) -> str:
    """Validate the absolute target-side data/ohlcv root path."""

    text = str(destination_root or "").strip()
    if not text:
        text = str(get_market_data_root_dir())
    if "\x00" in text or "\n" in text or "\r" in text or any(ch.isspace() for ch in text):
        raise ValueError("Destination root must not contain whitespace or control characters")
    if not text.startswith("/"):
        raise ValueError("Destination root must be an absolute path on the target host")
    if any(ch not in OHLCV_COPY_REMOTE_PATH_CHARS for ch in text):
        raise ValueError("Destination root contains unsupported characters")
    return text.rstrip("/") or "/"


def _parse_ohlcv_copy_ssh_args(ssh_command: Any) -> list[str]:
    """Parse the configured ssh command into argv for subprocess use."""

    text = str(ssh_command or "").strip() or "ssh"
    try:
        parts = shlex.split(text)
    except ValueError as exc:
        raise ValueError(f"Invalid SSH command: {exc}") from exc
    if not parts:
        parts = ["ssh"]
    if Path(parts[0]).name != "ssh":
        raise ValueError("SSH command must start with ssh")
    return parts


def _remote_ohlcv_copy_dir(destination_root: str, storage_name: str) -> str:
    """Build a POSIX target path for one exchange directory."""

    root = str(destination_root or "").rstrip("/") or "/"
    storage = str(storage_name or "").strip("/")
    return f"/{storage}" if root == "/" else f"{root}/{storage}"


def _build_ohlcv_copy_mkdir_command(*, target: str, ssh_args: list[str], remote_dir: str) -> list[str]:
    """Build the remote mkdir command for one exchange copy."""

    return list(ssh_args) + [str(target), "mkdir", "-p", str(remote_dir)]


def _build_ohlcv_copy_rsync_command(
    *,
    source_dir: Path,
    target: str,
    destination_root: str,
    storage_name: str,
    ssh_args: list[str],
    mode: str,
    dry_run: bool = False,
) -> list[str]:
    """Build the rsync argv for one exchange copy without using a local shell."""

    remote_dir = _remote_ohlcv_copy_dir(destination_root, storage_name)
    cmd = [
        "rsync",
        "-a",
        "--partial",
        "--partial-dir=.rsync-partial",
        "--delay-updates",
        "--human-readable",
        "--info=progress2,stats2",
    ]
    if dry_run:
        cmd.extend(["--dry-run", "--stats", "--itemize-changes"])
    if str(mode or "").strip().lower() == "missing_only":
        cmd.append("--ignore-existing")
    cmd.extend([
        "-e",
        shlex.join(list(ssh_args)),
        f"{source_dir}/",
        f"{target}:{remote_dir}/",
    ])
    return cmd


def _terminate_ohlcv_copy_process(proc: subprocess.Popen[str]) -> None:
    """Terminate a running copy subprocess and its process group."""

    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except Exception:
        try:
            proc.terminate()
        except Exception:
            pass
    try:
        proc.wait(timeout=10)
    except Exception:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


def _append_ohlcv_copy_output(job_id: str, raw_line: str) -> list[str]:
    """Append cleaned subprocess output lines to the job log and return them."""

    out: list[str] = []
    for part in str(raw_line or "").replace("\r", "\n").splitlines():
        text = part.strip()
        if text:
            out.append(text)
            _append_to_job_log(job_id, f"    {text}")
    return out


def _is_ohlcv_copy_rsync_stat_line(line: str) -> bool:
    """Return true when an rsync output line contains a summary stat."""

    text = str(line or "")
    return any(f"{label}:" in text for label in OHLCV_COPY_RSYNC_STAT_LABELS)


def _ohlcv_copy_stat_value(line: str, label: str) -> str:
    """Extract the value after one rsync stat label."""

    needle = f"{label}:"
    idx = str(line or "").find(needle)
    return "" if idx < 0 else str(line or "")[idx + len(needle):].strip()


def _parse_ohlcv_copy_count(value: Any) -> int | None:
    """Parse rsync integer counts with comma or dot thousands separators."""

    text = str(value or "").split("(", 1)[0].strip()
    if not text:
        return None
    text = text.replace(" ", "").replace(".", "").replace(",", "")
    try:
        return int(text)
    except Exception:
        return None


def _parse_ohlcv_copy_bytes(value: Any) -> int | None:
    """Parse rsync human-readable byte values into bytes."""

    import re

    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"([0-9][0-9.,]*)\s*([KMGTPE]?)(?:i?B|bytes)?", text, re.IGNORECASE)
    if not match:
        return None
    number_text = str(match.group(1) or "")
    unit = str(match.group(2) or "").upper()
    if "," in number_text:
        number_text = number_text.replace(".", "").replace(",", ".")
    elif number_text.count(".") > 1:
        number_text = number_text.replace(".", "")
    elif not unit and "." in number_text and len(number_text.rsplit(".", 1)[-1]) == 3:
        number_text = number_text.replace(".", "")
    try:
        number = float(number_text)
    except Exception:
        return None
    power = {"K": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}.get(unit, 0)
    return int(round(number * (1024 ** power)))


def _parse_ohlcv_copy_rsync_stats(lines: list[str]) -> dict[str, Any]:
    """Parse one rsync --stats output block."""

    out: dict[str, Any] = {
        "files_total": 0,
        "files_transferred": 0,
        "total_size_bytes": 0,
        "transfer_size_bytes": 0,
        "bytes_sent": 0,
        "bytes_received": 0,
        "stats_lines": [],
    }
    seen: set[str] = set()
    for line in lines or []:
        text = str(line or "").strip()
        if not _is_ohlcv_copy_rsync_stat_line(text):
            continue
        out["stats_lines"].append(text)
        if "Number of regular files transferred:" in text:
            value = _parse_ohlcv_copy_count(_ohlcv_copy_stat_value(text, "Number of regular files transferred"))
            if value is not None:
                out["files_transferred"] = value
                seen.add("files_transferred")
        elif "Number of files:" in text:
            value = _parse_ohlcv_copy_count(_ohlcv_copy_stat_value(text, "Number of files"))
            if value is not None:
                out["files_total"] = value
                seen.add("files_total")
        elif "Total transferred file size:" in text:
            value = _parse_ohlcv_copy_bytes(_ohlcv_copy_stat_value(text, "Total transferred file size"))
            if value is not None:
                out["transfer_size_bytes"] = value
                seen.add("transfer_size_bytes")
        elif "Total file size:" in text:
            value = _parse_ohlcv_copy_bytes(_ohlcv_copy_stat_value(text, "Total file size"))
            if value is not None:
                out["total_size_bytes"] = value
                seen.add("total_size_bytes")
        elif "Total bytes sent:" in text:
            value = _parse_ohlcv_copy_bytes(_ohlcv_copy_stat_value(text, "Total bytes sent"))
            if value is not None:
                out["bytes_sent"] = value
                seen.add("bytes_sent")
        elif "Total bytes received:" in text:
            value = _parse_ohlcv_copy_bytes(_ohlcv_copy_stat_value(text, "Total bytes received"))
            if value is not None:
                out["bytes_received"] = value
                seen.add("bytes_received")
    out["seen"] = sorted(seen)
    return out


def _build_ohlcv_copy_dry_run_result(stats: dict[str, Any], *, copied: list[str], skipped: list[str], duration_s: int) -> dict[str, Any]:
    """Build the structured dry-run result stored in the job progress."""

    return {
        "dry_run": True,
        "exchanges": list(copied),
        "skipped_exchanges": list(skipped),
        "remote_paths": list(stats.get("remote_paths") or []),
        "files_total": int(stats.get("files_total") or 0),
        "files_transferred": int(stats.get("files_transferred") or 0),
        "total_size_bytes": int(stats.get("total_size_bytes") or 0),
        "transfer_size_bytes": int(stats.get("transfer_size_bytes") or 0),
        "bytes_sent": int(stats.get("bytes_sent") or 0),
        "bytes_received": int(stats.get("bytes_received") or 0),
        "exchange_stats": list(stats.get("exchange_stats") or []),
        "duration_s": int(duration_s),
    }


def _run_ohlcv_copy_command(job_path: Path, job_id: str, cmd: list[str], label: str) -> list[str]:
    """Run one mkdir/rsync command, streaming output and honoring cancellation."""

    stats_lines: list[str] = []
    _append_to_job_log(job_id, f"  {label}: {shlex.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        cwd=str(Path(__file__).resolve().parent),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    try:
        while True:
            if _STOP or _is_cancel_requested(job_path):
                _terminate_ohlcv_copy_process(proc)
                raise RuntimeError("cancelled")
            if proc.stdout:
                ready, _, _ = select.select([proc.stdout], [], [], 0.5)
                if ready:
                    line = proc.stdout.readline()
                    if line:
                        for text in _append_ohlcv_copy_output(job_id, line):
                            if _is_ohlcv_copy_rsync_stat_line(text):
                                stats_lines.append(text)
                        continue
            rc = proc.poll()
            if rc is not None:
                break
        if proc.stdout:
            for line in proc.stdout.readlines():
                for text in _append_ohlcv_copy_output(job_id, line):
                    if _is_ohlcv_copy_rsync_stat_line(text):
                        stats_lines.append(text)
        if proc.returncode != 0:
            raise RuntimeError(f"{label} failed with exit code {proc.returncode}")
        return stats_lines
    finally:
        if proc.poll() is None:
            _terminate_ohlcv_copy_process(proc)


def _run_ohlcv_copy(job_path: Path, payload: dict[str, Any], *, dry_run: bool = False) -> None:
    """Copy selected local OHLCV exchange directories to a remote PBGui data root."""

    started_ts = time.time()
    job_id = job_path.stem
    dry_run = bool(dry_run or payload.get("dry_run"))
    target = _normalize_ohlcv_copy_target(payload.get("target"))
    destination_root = _normalize_ohlcv_copy_destination_root(payload.get("destination_root"))
    ssh_args = _parse_ohlcv_copy_ssh_args(payload.get("ssh_command"))
    if len(ssh_args) > 1 and ssh_args[-1] == target:
        raise ValueError("SSH command must not include the target host")
    mode = str(payload.get("mode") or "missing_only").strip().lower()
    if mode not in OHLCV_COPY_MODES:
        raise ValueError("Invalid OHLCV copy mode")
    exchanges = _normalize_ohlcv_copy_exchanges(payload.get("exchanges"))
    if shutil.which("rsync") is None:
        raise RuntimeError("rsync is not installed or not available in PATH")

    total_steps = max(1, len(exchanges))
    step_i = 0
    copied: list[str] = []
    skipped: list[str] = []
    source_root = get_market_data_root_dir()
    dry_run_totals: dict[str, Any] = {
        "remote_paths": [],
        "files_total": 0,
        "files_transferred": 0,
        "total_size_bytes": 0,
        "transfer_size_bytes": 0,
        "bytes_sent": 0,
        "bytes_received": 0,
        "exchange_stats": [],
    }

    _init_job_log(job_id)
    _append_to_job_log(
        job_id,
        f"job started  target={target}  destination_root={destination_root}  exchanges={exchanges}  mode={mode}  dry_run={1 if dry_run else 0}",
    )
    _append_to_job_log(job_id, "safety  delete=disabled  missing_only=" + ("1" if mode == "missing_only" else "0") + "  writes=" + ("0" if dry_run else "1"))

    def update_progress(**kw: Any) -> None:
        def mut(o: dict[str, Any]) -> None:
            pr = o.get("progress")
            pr = pr if isinstance(pr, dict) else {}
            pr.update(kw)
            pr["step"] = int(step_i)
            pr["total"] = int(total_steps)
            pr["mode"] = "ohlcv_copy_dry_run" if dry_run else "ohlcv_copy"
            o["progress"] = pr
        update_job_file(job_path, mutate=mut)

    update_progress(stage="starting", target=target, destination_root=destination_root, copied_exchanges=[], skipped_exchanges=[])

    for exchange in exchanges:
        if _STOP:
            raise RuntimeError("Worker stopping")
        if _is_cancel_requested(job_path):
            raise RuntimeError("cancelled")
        step_i += 1
        meta = OHLCV_COPY_EXCHANGES[exchange]
        storage = meta["storage"]
        label = meta["label"]
        source_dir = source_root / storage
        remote_dir = _remote_ohlcv_copy_dir(destination_root, storage)
        update_progress(stage="preparing", exchange=exchange, storage=storage, remote_dir=remote_dir)

        if not source_dir.is_dir():
            skipped.append(exchange)
            _append_to_job_log(job_id, f"[{step_i}/{total_steps}] {label} skipped  missing_source={source_dir}")
            update_progress(stage="skipped", skipped_exchanges=list(skipped))
            continue

        action_label = "dry run" if dry_run else "copy"
        _append_to_job_log(job_id, f"[{step_i}/{total_steps}] {label} {action_label} starting  source={source_dir}  remote={target}:{remote_dir}/")
        if dry_run:
            _append_to_job_log(job_id, f"  {label} mkdir skipped for dry run; no remote directories or files will be created")
        else:
            mkdir_cmd = _build_ohlcv_copy_mkdir_command(target=target, ssh_args=ssh_args, remote_dir=remote_dir)
            update_progress(stage="mkdir", exchange=exchange, storage=storage, remote_dir=remote_dir)
            _run_ohlcv_copy_command(job_path, job_id, mkdir_cmd, f"{label} mkdir")

        rsync_cmd = _build_ohlcv_copy_rsync_command(
            source_dir=source_dir,
            target=target,
            destination_root=destination_root,
            storage_name=storage,
            ssh_args=ssh_args,
            mode=mode,
            dry_run=dry_run,
        )
        update_progress(stage="rsync_dry_run" if dry_run else "rsync", exchange=exchange, storage=storage, remote_dir=remote_dir)
        rsync_stats_lines = _run_ohlcv_copy_command(job_path, job_id, rsync_cmd, f"{label} rsync{' dry run' if dry_run else ''}") or []
        copied.append(exchange)
        progress_payload: dict[str, Any] = {
            "stage": "running",
            "copied_exchanges": list(copied),
            "skipped_exchanges": list(skipped),
            "last_exchange": exchange,
        }
        if dry_run:
            exchange_stats = _parse_ohlcv_copy_rsync_stats(rsync_stats_lines)
            exchange_stats.update(
                {
                    "exchange": exchange,
                    "label": label,
                    "remote_path": f"{target}:{remote_dir}/",
                }
            )
            dry_run_totals["remote_paths"].append(f"{target}:{remote_dir}/")
            dry_run_totals["exchange_stats"].append(exchange_stats)
            for key in ("files_total", "files_transferred", "total_size_bytes", "transfer_size_bytes", "bytes_sent", "bytes_received"):
                dry_run_totals[key] = int(dry_run_totals.get(key) or 0) + int(exchange_stats.get(key) or 0)
            progress_payload["last_result"] = _build_ohlcv_copy_dry_run_result(
                dry_run_totals,
                copied=copied,
                skipped=skipped,
                duration_s=int(max(0, time.time() - started_ts)),
            )
        update_progress(**progress_payload)
        append_exchange_download_log("ohlcv", f"[{'ohlcv_copy_dry_run' if dry_run else 'ohlcv_copy'}] {label} {'dry run' if dry_run else 'copied'} to {target}:{remote_dir}/ mode={mode}")
        _append_to_job_log(job_id, f"[{step_i}/{total_steps}] {label} {'dry run ' if dry_run else ''}done")

    if not copied:
        raise RuntimeError("No selected source exchange directories exist")

    duration_s = int(max(0, time.time() - started_ts))
    if dry_run:
        last_result = _build_ohlcv_copy_dry_run_result(dry_run_totals, copied=copied, skipped=skipped, duration_s=duration_s)
    else:
        last_result = {"copied": len(copied), "skipped": len(skipped), "duration_s": duration_s}
    update_progress(
        stage="done",
        copied_exchanges=list(copied),
        skipped_exchanges=list(skipped),
        last_result=last_result,
    )
    _append_to_job_log(job_id, f"job finished  {'dry_run_exchanges' if dry_run else 'copied'}={len(copied)}  skipped={len(skipped)}  duration={int(time.time()-started_ts)}s")


def _run_hl_aws_l2book_auto(job_path: Path, payload: dict[str, Any]) -> None:
    job_id = job_path.stem
    profile = str(payload.get("profile") or "pbgui-hyperliquid").strip() or "pbgui-hyperliquid"
    region = str(payload.get("region") or "").strip()
    coins = payload.get("coins")
    coins = coins if isinstance(coins, list) else []
    # Keep original coin names (normalization happens in download function for S3 keys only)
    coins = [str(c).strip() for c in coins if str(c).strip()]

    if not coins:
        raise ValueError("No coins in job")

    only_missing_1m_src_hours = bool(payload.get("only_missing_1m_src_hours", True))

    chunk_days = int(payload.get("chunk_days") or 7)
    if chunk_days < 1:
        chunk_days = 7

    creds = load_aws_profile_credentials(profile)
    ak = str(creds.get("aws_access_key_id") or "").strip()
    sk = str(creds.get("aws_secret_access_key") or "").strip()
    if not ak or not sk:
        raise RuntimeError(f"Missing AWS credentials in profile '{profile}'")

    if not region:
        region = load_aws_profile_region(profile) or ""
    if not region:
        region = "us-east-2"

    def _get_ini_int(section: str, key: str, default: int) -> int:
        raw = load_ini(section, key)
        try:
            val = int(str(raw).strip())
            if val >= 1:
                return val
        except Exception:
            pass
        return int(default)

    def _get_ini_float(section: str, key: str, default: float) -> float:
        raw = load_ini(section, key)
        try:
            val = float(str(raw).strip())
            if val > 0:
                return val
        except Exception:
            pass
        return float(default)

    l2book_timeout_s = _get_ini_float("market_data", "hl_l2book_scan_timeout_s", 5.0)
    l2book_workers = _get_ini_int("market_data", "hl_l2book_scan_workers", 8)
    ini_path = Path("pbgui.ini")
    ini_mtime: float | None = None

    def _reload_l2book_settings() -> bool:
        nonlocal ini_mtime, l2book_timeout_s, l2book_workers
        try:
            if not ini_path.exists():
                return False
            mtime = ini_path.stat().st_mtime
            if ini_mtime is not None and mtime == ini_mtime:
                return False
            ini_mtime = mtime
        except Exception:
            return False

        new_timeout = _get_ini_float("market_data", "hl_l2book_scan_timeout_s", 5.0)
        new_workers = _get_ini_int("market_data", "hl_l2book_scan_workers", 8)
        changed = (new_timeout != l2book_timeout_s) or (new_workers != l2book_workers)
        l2book_timeout_s = new_timeout
        l2book_workers = new_workers
        return changed

    # Determine archive range
    start_day = str(payload.get("start_day") or "").strip()
    end_day = str(payload.get("end_day") or "").strip()
    if not start_day or not end_day:
        oldest, newest = get_hyperliquid_archive_day_range_aws(
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            region_name=region,
        )
        start_day, end_day = str(oldest), str(newest)

    _job_log(
        "hl_aws_l2book_auto start profile=%s region=%s range=%s->%s coins=%s timeout_s=%s workers=%s only_missing_1m_src_hours=%s"
        % (profile, region, start_day, end_day, len(coins), l2book_timeout_s, l2book_workers, only_missing_1m_src_hours)
    )
    _init_job_log(job_id)
    _append_to_job_log(job_id, f"job started  coins={coins}  range={start_day}->{end_day}  profile={profile}  region={region}")

    days = _iter_days(start_day, end_day)
    coin_days: dict[str, list[str]] = {}
    for coin in coins:
        filtered = list(days)
        if only_missing_1m_src_hours:
            coin_dir = normalize_market_data_coin_dir("hyperliquid", coin)
            oldest_l2book_day = (
                get_oldest_day_with_source_code(
                    exchange="hyperliquid",
                    coin=coin_dir,
                    code=SOURCE_CODE_L2BOOK,
                )
                if coin_dir
                else None
            )
            if oldest_l2book_day:
                filtered = [d for d in filtered if d >= str(oldest_l2book_day)]
                if len(filtered) != len(days):
                    append_exchange_download_log(
                        "hyperliquid",
                        f"[INFO] [hl_aws_l2book_auto] {coin} skip_older_than_l2book_oldest={oldest_l2book_day} filtered_days={len(days)-len(filtered)}",
                    )
        coin_days[coin] = filtered

    total_steps = max(1, sum(len(v) for v in coin_days.values()))
    step_i = 0

    downloaded_total = 0
    skipped_total = 0
    failed_total = 0
    downloaded_bytes_total = 0
    skipped_bytes_total = 0
    failed_bytes_total = 0
    
    # Track coins with actual downloads to trigger Build OHLCV jobs
    coins_with_downloads: dict[str, int] = {}

    def update_progress(
        *,
        downloaded_total_override: int | None = None,
        skipped_total_override: int | None = None,
        failed_total_override: int | None = None,
        downloaded_bytes_override: int | None = None,
        skipped_bytes_override: int | None = None,
        failed_bytes_override: int | None = None,
        chunk_done: int | None = None,
        chunk_total: int | None = None,
        **kw,
    ):
        def mut(o):
            pr = o.get("progress")
            pr = pr if isinstance(pr, dict) else {}
            pr.update(kw)
            pr["step"] = int(step_i)
            pr["total"] = int(total_steps)
            pr["downloaded_total"] = int(downloaded_total if downloaded_total_override is None else downloaded_total_override)
            pr["skipped_existing_total"] = int(skipped_total if skipped_total_override is None else skipped_total_override)
            pr["failed_total"] = int(failed_total if failed_total_override is None else failed_total_override)
            pr["downloaded_bytes_total"] = int(
                downloaded_bytes_total if downloaded_bytes_override is None else downloaded_bytes_override
            )
            pr["skipped_existing_bytes_total"] = int(
                skipped_bytes_total if skipped_bytes_override is None else skipped_bytes_override
            )
            pr["failed_bytes_total"] = int(failed_bytes_total if failed_bytes_override is None else failed_bytes_override)
            if chunk_done is not None:
                pr["chunk_done"] = int(chunk_done)
            if chunk_total is not None:
                pr["chunk_total"] = int(chunk_total)
            o["progress"] = pr

        update_job_file(job_path, mutate=mut)

    update_progress(stage="starting", recent_keys=[])
    update_progress(stage="starting", recent_failed=[])

    for coin in coins:
        for day in coin_days.get(coin, []):
            if _STOP:
                raise RuntimeError("Worker stopping")
            if _is_cancel_requested(job_path):
                raise RuntimeError("cancelled")

            if _reload_l2book_settings():
                _job_log(
                    "hl_aws_l2book_auto updated settings timeout_s=%s workers=%s"
                    % (l2book_timeout_s, l2book_workers)
                )
            step_i += 1
            update_progress(coin=coin, chunk_start=day, chunk_end=day)

            hours_to_download: list[int] | None = None
            if only_missing_1m_src_hours:
                hours_to_download = _hours_missing_in_1m_src(coin=coin, day=day)
                if not hours_to_download:
                    res = {
                        "planned": 0,
                        "downloaded": 0,
                        "skipped_existing": 0,
                        "failed": 0,
                        "filtered_existing_1m_src_hours": 24,
                    }
                    append_exchange_download_log(
                        "hyperliquid",
                        f"[INFO] [hl_aws_l2book_auto] {coin} {day} skipped (1m_src already covers all hours)",
                    )
                    update_progress(last_result=res, stage="running", chunk_done=0, chunk_total=0)
                    continue

            # Download all hours (0-23) - missing files will be handled as 404 during download
            last_cb = 0.0
            cb_lock = threading.Lock()

            def progress_cb(snap: dict[str, Any]) -> None:
                nonlocal last_cb
                with cb_lock:
                    now = time.time()
                    if now - last_cb < 1.0 and int(snap.get("done", 0)) != int(snap.get("planned", 0)):
                        return
                    last_cb = now

                disp_downloaded = downloaded_total + int(snap.get("downloaded", 0))
                disp_skipped = skipped_total + int(snap.get("skipped_existing", 0))
                disp_failed = failed_total + int(snap.get("failed", 0))
                disp_dl_b = downloaded_bytes_total + int(snap.get("downloaded_bytes", 0))
                disp_sk_b = skipped_bytes_total + int(snap.get("skipped_existing_bytes", 0))
                disp_fl_b = failed_bytes_total + int(snap.get("failed_bytes", 0))

                update_progress(
                    stage="running",
                    recent_keys=snap.get("recent_keys"),
                    recent_failed=snap.get("recent_failed"),
                    active_downloads=snap.get("active_downloads"),
                    downloaded_total_override=disp_downloaded,
                    skipped_total_override=disp_skipped,
                    failed_total_override=disp_failed,
                    downloaded_bytes_override=disp_dl_b,
                    skipped_bytes_override=disp_sk_b,
                    failed_bytes_override=disp_fl_b,
                    chunk_done=int(snap.get("done", 0)),
                    chunk_total=int(snap.get("planned", 0)),
                )

            res = download_hyperliquid_l2book_aws(
                coin=coin,
                start_date=day,
                end_date=day,
                aws_access_key_id=ak,
                aws_secret_access_key=sk,
                region_name=region,
                overwrite=False,
                recent_keys_limit=8,
                progress_cb=progress_cb,
                hours=hours_to_download,
            )
            # Log concise summary instead of full result dict
            summary = (
                f"planned:{res.get('planned',0)} "
                f"downloaded:{res.get('downloaded',0)} "
                f"skipped:{res.get('skipped_existing',0)} "
                f"failed:{res.get('failed',0)}"
            )
            if res.get('total_bytes', 0) > 0:
                mb = res['total_bytes'] / (1024 * 1024)
                summary += f" ({mb:.1f} MB)"
            append_exchange_download_log("hyperliquid", f"[INFO] [hl_aws_l2book_auto] {coin} {day} {summary}")
            _append_to_job_log(job_id, f"  {coin}  {day}  {summary}")

            try:
                dl_count = int(res.get("downloaded", 0))
                downloaded_total += dl_count
                skipped_total += int(res.get("skipped_existing", 0))
                failed_total += int(res.get("failed", 0))
                downloaded_bytes_total += int(res.get("downloaded_bytes", 0))
                skipped_bytes_total += int(res.get("skipped_existing_bytes", 0))
                failed_bytes_total += int(res.get("failed_bytes", 0))
                
                # Track coins with actual downloads
                if dl_count > 0:
                    coins_with_downloads[coin] = coins_with_downloads.get(coin, 0) + dl_count
            except Exception:
                pass
            if isinstance(res.get("recent_keys"), list):
                update_progress(
                    last_result=res,
                    stage="running",
                    recent_keys=res.get("recent_keys"),
                    recent_failed=res.get("recent_failed"),
                    active_downloads=[],
                    chunk_done=int(res.get("planned", 0)),
                    chunk_total=int(res.get("planned", 0)),
                )
            else:
                update_progress(last_result=res, stage="running")
        # After all days for this coin: refresh l2Book inventory cache
        try:
            _refresh_inventory_coin("hyperliquid", "l2Book", coin)
        except Exception:
            pass
    
    _append_to_job_log(job_id, f"job finished  downloaded={downloaded_total}  skipped={skipped_total}  failed={failed_total}")

    # After all downloads: trigger Build OHLCV jobs for coins with new data
    if coins_with_downloads:
        end_day = datetime.utcnow().strftime("%Y%m%d")
        for coin in sorted(coins_with_downloads.keys()):
            try:
                job = enqueue_job(
                    job_type="hl_best_1m",
                    exchange="hyperliquid",
                    payload={
                        "coins": [coin],
                        "end_day": end_day,
                    },
                )
                append_exchange_download_log(
                    "hyperliquid",
                    f"[hl_aws_l2book_auto] Auto-triggered Build OHLCV for {coin}, job_id={job.job_id}",
                )
            except Exception as e:
                append_exchange_download_log(
                    "hyperliquid",
                    f"[hl_aws_l2book_auto] Failed to trigger Build OHLCV for {coin}: {e}",
                )


def _run_hl_best_1m(job_path: Path, payload: dict[str, Any]) -> None:
    started_ts = time.time()
    job_id = job_path.stem
    coins = payload.get("coins")
    coins = coins if isinstance(coins, list) else []
    coins = [str(c).strip().upper() for c in coins if str(c).strip()]

    if not coins:
        raise ValueError("No coins in job")

    end_day = str(payload.get("end_day") or "").strip()
    if not end_day:
        end_day = datetime.utcnow().strftime("%Y%m%d")
    start_day = str(payload.get("start_day") or "").strip()
    refetch = bool(payload.get("refetch") or False)

    total_steps = max(1, len(coins))
    step_i = 0

    _init_job_log(job_id)
    _append_to_job_log(job_id, f"job started  coins={coins}  end_day={end_day}  start_day={start_day or 'inception'}  refetch={refetch}")

    def update_progress(**kw):
        def mut(o):
            pr = o.get("progress")
            pr = pr if isinstance(pr, dict) else {}
            pr.update(kw)
            pr["step"] = int(step_i)
            pr["total"] = int(total_steps)
            pr["mode"] = "improve"
            o["progress"] = pr

        update_job_file(job_path, mutate=mut)

    update_progress(
        stage="starting",
        last_result={
            "days_checked": 0,
            "l2book_minutes_added": 0,
            "binance_minutes_filled": 0,
            "bybit_minutes_filled": 0,
            "duration_s": 0,
        },
        last_binance_fill_day="",
        day="",
    )

    for coin in coins:
        if _STOP:
            raise RuntimeError("Worker stopping")
        if _is_cancel_requested(job_path):
            raise RuntimeError("cancelled")
        step_i += 1
        _append_to_job_log(job_id, f"[{step_i}/{total_steps}] {coin}  starting")
        stock_coin = bool(_is_stock_perp_coin(coin))
        update_progress(
            stage="running",
            coin=coin,
            chunk_done=0,
            chunk_total=0,
            last_result={
                "days_checked": 0,
                "tiingo_minutes_filled": 0,
                "tiingo_month_requests_used": 0,
                "l2book_minutes_added": 0 if not stock_coin else None,
                "binance_minutes_filled": 0 if not stock_coin else None,
                "bybit_minutes_filled": 0 if not stock_coin else None,
                "duration_s": int(max(0, time.time() - started_ts)),
            },
            last_binance_fill_day="",
            day="",
        )
        last_chunk_update = 0.0
        last_log_stage: list[str] = [""]   # mutable cell for closure
        last_log_ts: list[float] = [0.0]

        def progress_cb(snap: dict[str, Any]) -> None:
            nonlocal last_chunk_update
            now = time.time()
            stage = str(snap.get("stage") or "running")

            # Log stage transitions and periodic heartbeat (every 60s)
            prev_stage = last_log_stage[0]
            if stage != prev_stage or now - last_log_ts[0] >= 60.0:
                day = str(snap.get("day") or snap.get("month_key") or "")
                done = snap.get("done")
                total = snap.get("total_days") or snap.get("planned")
                extra = ""
                if day:
                    extra += f"  day={day}"
                if done is not None and total is not None:
                    extra += f"  {done}/{total}"
                if snap.get("tiingo_wait_s") is not None:
                    extra += f"  wait={snap['tiingo_wait_s']}s ({snap.get('tiingo_wait_reason','')})"
                _append_to_job_log(job_id, f"    {coin}  stage={stage}{extra}")
                last_log_stage[0] = stage
                last_log_ts[0] = now

            if now - last_chunk_update < 0.5:
                return
            last_chunk_update = now
            planned = snap.get("planned")
            done = snap.get("done")
            total_days = snap.get("total_days")
            stage = str(snap.get("stage") or "running")
            kw = {"stage": stage}
            if total_days is not None:
                kw["chunk_total"] = int(total_days)
            elif planned is not None:
                kw["chunk_total"] = int(planned)
            if done is not None:
                kw["chunk_done"] = int(done)
            if snap.get("day"):
                kw["day"] = str(snap.get("day"))
            if snap.get("month_key"):
                kw["month_key"] = str(snap.get("month_key"))
            if snap.get("month_day_index") is not None:
                kw["month_day_index"] = int(snap.get("month_day_index") or 0)
            if snap.get("month_day_total") is not None:
                kw["month_day_total"] = int(snap.get("month_day_total") or 0)
            if snap.get("ticker"):
                kw["ticker"] = str(snap.get("ticker"))
            if snap.get("tiingo_wait_s") is not None:
                kw["tiingo_wait_s"] = int(snap.get("tiingo_wait_s") or 0)
            if snap.get("tiingo_wait_reason") is not None:
                kw["tiingo_wait_reason"] = str(snap.get("tiingo_wait_reason") or "")
            if snap.get("tiingo_wait_kind") is not None:
                kw["tiingo_wait_kind"] = str(snap.get("tiingo_wait_kind") or "")
            if stage == "binance_fill" and snap.get("day"):
                kw["last_binance_fill_day"] = str(snap.get("day"))
            if any(k in snap for k in ("days_checked", "l2book_minutes_added", "binance_minutes_filled", "bybit_minutes_filled", "tiingo_minutes_filled", "tiingo_month_requests_used")):
                if stock_coin:
                    kw["last_result"] = {
                        "days_checked": int(snap.get("days_checked") or 0),
                        "tiingo_minutes_filled": int(snap.get("tiingo_minutes_filled") or 0),
                        "tiingo_month_requests_used": int(snap.get("tiingo_month_requests_used") or 0),
                        "duration_s": int(max(0, time.time() - started_ts)),
                    }
                else:
                    kw["last_result"] = {
                        "days_checked": int(snap.get("days_checked") or 0),
                        "l2book_minutes_added": int(snap.get("l2book_minutes_added") or 0),
                        "binance_minutes_filled": int(snap.get("binance_minutes_filled") or 0),
                        "bybit_minutes_filled": int(snap.get("bybit_minutes_filled") or 0),
                        "duration_s": int(max(0, time.time() - started_ts)),
                    }
            update_progress(**kw)

        def _job_stop_check() -> bool:
            return bool(_STOP or _is_cancel_requested(job_path))

        try:
            res = improve_best_hyperliquid_1m_archive_for_coin(
                coin=coin,
                end_date=end_day,
                start_date_override=start_day or None,
                dry_run=False,
                refetch=refetch,
                progress_cb=progress_cb,
                stop_check=_job_stop_check,
            )
        except RuntimeError as e:
            if _is_cancel_requested(job_path):
                raise RuntimeError("cancelled") from e
            raise
        except Exception as e:
            _append_to_job_log(job_id, f"  {coin}  ERROR {e}")
            raise
        out = res.to_dict()
        if isinstance(out, dict):
            out["duration_s"] = int(max(0, time.time() - started_ts))
            if stock_coin:
                out.pop("l2book_minutes_added", None)
                out.pop("binance_minutes_filled", None)
                out.pop("bybit_minutes_filled", None)
        append_exchange_download_log("hyperliquid", f"[INFO] [hl_best_1m_job] {coin} {out}")
        update_progress(stage="running", last_result=out)
        _append_to_job_log(job_id, f"  {coin}  done  duration_s={int(time.time()-started_ts)}")
        try:
            _refresh_inventory_coin("hyperliquid", "1m", coin)
        except Exception:
            pass

    _append_to_job_log(job_id, f"job finished  duration={int(time.time()-started_ts)}s")


def _run_binance_best_1m(job_path: Path, payload: dict[str, Any]) -> None:
    started_ts = time.time()
    job_id = job_path.stem
    coins = payload.get("coins")
    coins = coins if isinstance(coins, list) else []
    coins = [str(c).strip().upper() for c in coins if str(c).strip()]

    if not coins:
        raise ValueError("No coins in job")

    end_day = str(payload.get("end_day") or "").strip()
    if not end_day:
        end_day = datetime.utcnow().strftime("%Y%m%d")
    start_day = str(payload.get("start_day") or "").strip()
    refetch = bool(payload.get("refetch") or False)

    total_steps = max(1, len(coins))
    step_i = 0

    _init_job_log(job_id)
    _append_to_job_log(job_id, f"job started  coins={coins}  end_day={end_day}  start_day={start_day or 'inception'}  refetch={refetch}")

    def update_progress(**kw):
        def mut(o):
            pr = o.get("progress")
            pr = pr if isinstance(pr, dict) else {}
            pr.update(kw)
            pr["step"] = int(step_i)
            pr["total"] = int(total_steps)
            pr["mode"] = "binance_best_1m"
            o["progress"] = pr
        update_job_file(job_path, mutate=mut)

    update_progress(stage="starting", last_result={"days_checked": 0, "minutes_written": 0, "duration_s": 0})

    for coin in coins:
        if _STOP:
            raise RuntimeError("Worker stopping")
        if _is_cancel_requested(job_path):
            raise RuntimeError("cancelled")
        step_i += 1
        _append_to_job_log(job_id, f"[{step_i}/{total_steps}] {coin}  starting")
        update_progress(stage="running", coin=coin, chunk_done=0, chunk_total=0,
                        last_result={"days_checked": 0, "minutes_written": 0,
                                     "duration_s": int(max(0, time.time() - started_ts))})

        last_chunk_update = 0.0
        last_logged_stage = ""
        last_log_ts2: list[float] = [0.0]

        def progress_cb(snap: dict[str, Any], _coin=coin) -> None:
            nonlocal last_chunk_update, last_logged_stage
            now = time.time()
            stage = str(snap.get("stage") or "running")
            # Log stage transitions and periodic heartbeat (every 60s)
            if stage != last_logged_stage or now - last_log_ts2[0] >= 60.0:
                last_logged_stage = stage
                last_log_ts2[0] = now
                extra = ""
                if snap.get("day"):
                    extra = f"  day={snap['day']}"
                elif snap.get("month_key"):
                    extra = f"  month={snap['month_key']}"
                elif snap.get("first_archive"):
                    extra = f"  first_archive={snap['first_archive']}"
                done = snap.get("done")
                total = snap.get("total_days")
                if done is not None and total is not None:
                    extra += f"  {done}/{total}"
                _append_to_job_log(job_id, f"  {_coin}  stage={stage}{extra}")
            if now - last_chunk_update < 0.5:
                return
            last_chunk_update = now
            kw: dict[str, Any] = {"stage": stage}
            if snap.get("day"):
                kw["day"] = str(snap["day"])
            if snap.get("month_key"):
                kw["month_key"] = str(snap["month_key"])
            if snap.get("month_day_index") is not None:
                kw["month_day_index"] = int(snap["month_day_index"])
            if snap.get("month_day_total") is not None:
                kw["month_day_total"] = int(snap["month_day_total"])
            done = snap.get("done")
            if done is not None:
                total_days = snap.get("total_days")
                kw["chunk_done"] = int(done)
                kw["chunk_total"] = int(total_days) if total_days else int(total_steps * 100)
            if any(k in snap for k in ("days_checked", "minutes_written")):
                kw["last_result"] = {
                    "days_checked": int(snap.get("days_checked") or 0),
                    "minutes_written": int(snap.get("minutes_written") or 0),
                    "duration_s": int(max(0, time.time() - started_ts)),
                }
            update_progress(**kw)

        def _job_stop_check() -> bool:
            return bool(_STOP or _is_cancel_requested(job_path))

        try:
            res = improve_best_binance_1m_for_coin(
                coin=coin,
                end_date=end_day,
                start_date_override=start_day or None,
                refetch=refetch,
                progress_cb=progress_cb,
                stop_check=_job_stop_check,
            )
        except RuntimeError as e:
            _append_to_job_log(job_id, f"  {coin}  ERROR {e}")
            if _is_cancel_requested(job_path):
                raise RuntimeError("cancelled") from e
            raise
        out = res.to_dict()
        if isinstance(out, dict):
            out["duration_s"] = int(max(0, time.time() - started_ts))
        append_exchange_download_log("binanceusdm", f"[INFO] [binance_best_1m_job] {coin} {out}")
        _append_to_job_log(job_id, f"  {coin}  done  days_checked={out.get('days_checked', 0)}  minutes_written={out.get('minutes_written', 0)}  notes={out.get('notes', [])}")
        update_progress(stage="running", last_result=out)
        try:
            _refresh_inventory_coin("binanceusdm", "1m", coin)
        except Exception:
            pass

    _append_to_job_log(job_id, f"job finished  duration={int(time.time()-started_ts)}s")


def _run_bybit_best_1m(job_path: Path, payload: dict[str, Any]) -> None:
    started_ts = time.time()
    job_id = job_path.stem
    coins = payload.get("coins")
    coins = coins if isinstance(coins, list) else []
    coins = [str(c).strip().upper() for c in coins if str(c).strip()]

    if not coins:
        raise ValueError("No coins in job")

    end_day = str(payload.get("end_day") or "").strip()
    if not end_day:
        end_day = datetime.utcnow().strftime("%Y%m%d")
    start_day = str(payload.get("start_day") or "").strip()
    refetch = bool(payload.get("refetch") or False)

    total_steps = max(1, len(coins))
    step_i = 0

    _init_job_log(job_id)
    _append_to_job_log(job_id, f"job started  coins={coins}  end_day={end_day}  start_day={start_day or 'inception'}  refetch={refetch}")

    def update_progress(**kw):
        def mut(o):
            pr = o.get("progress")
            pr = pr if isinstance(pr, dict) else {}
            pr.update(kw)
            pr["step"] = int(step_i)
            pr["total"] = int(total_steps)
            pr["mode"] = "bybit_best_1m"
            o["progress"] = pr
        update_job_file(job_path, mutate=mut)

    update_progress(stage="starting", last_result={"days_checked": 0, "minutes_written": 0, "duration_s": 0})

    for coin in coins:
        if _STOP:
            raise RuntimeError("Worker stopping")
        if _is_cancel_requested(job_path):
            raise RuntimeError("cancelled")
        step_i += 1
        _append_to_job_log(job_id, f"[{step_i}/{total_steps}] {coin}  starting")
        update_progress(stage="running", coin=coin, chunk_done=0, chunk_total=0,
                        last_result={"days_checked": 0, "minutes_written": 0,
                                     "duration_s": int(max(0, time.time() - started_ts))})

        last_chunk_update = 0.0
        last_logged_stage = ""
        last_log_ts2: list[float] = [0.0]

        def progress_cb(snap: dict[str, Any], _coin=coin) -> None:
            nonlocal last_chunk_update, last_logged_stage
            now = time.time()
            stage = str(snap.get("stage") or "running")
            # Log stage transitions and periodic heartbeat (every 60s)
            if stage != last_logged_stage or now - last_log_ts2[0] >= 60.0:
                last_logged_stage = stage
                last_log_ts2[0] = now
                extra = ""
                if snap.get("day"):
                    extra = f"  day={snap['day']}"
                elif snap.get("first_archive"):
                    extra = f"  first_archive={snap['first_archive']}"
                done = snap.get("done")
                total = snap.get("total_days")
                if done is not None and total is not None:
                    extra += f"  {done}/{total}"
                _append_to_job_log(job_id, f"  {_coin}  stage={stage}{extra}")
            if now - last_chunk_update < 0.5:
                return
            last_chunk_update = now
            kw: dict[str, Any] = {"stage": stage}
            if snap.get("day"):
                kw["day"] = str(snap["day"])
            done = snap.get("done")
            if done is not None:
                total_days = snap.get("total_days")
                kw["chunk_done"] = int(done)
                kw["chunk_total"] = int(total_days) if total_days else int(total_steps * 100)
            if any(k in snap for k in ("days_checked", "minutes_written", "ccxt_days_fetched")):
                kw["last_result"] = {
                    "days_checked": int(snap.get("days_checked") or 0),
                    "ccxt_days_fetched": int(snap.get("ccxt_days_fetched") or 0),
                    "minutes_written": int(snap.get("minutes_written") or 0),
                    "duration_s": int(max(0, time.time() - started_ts)),
                }
            update_progress(**kw)

        def _job_stop_check() -> bool:
            return bool(_STOP or _is_cancel_requested(job_path))

        try:
            res = improve_best_bybit_1m_for_coin(
                coin=coin,
                end_date=end_day,
                start_date_override=start_day or None,
                refetch=refetch,
                progress_cb=progress_cb,
                stop_check=_job_stop_check,
            )
        except RuntimeError as e:
            _append_to_job_log(job_id, f"  {coin}  ERROR {e}")
            if _is_cancel_requested(job_path):
                raise RuntimeError("cancelled") from e
            raise
        out = res.to_dict()
        if isinstance(out, dict):
            out["duration_s"] = int(max(0, time.time() - started_ts))
        append_exchange_download_log("bybit", f"[INFO] [bybit_best_1m_job] {coin} {out}")
        _append_to_job_log(job_id, f"  {coin}  done  days_checked={out.get('days_checked', 0)}  ccxt_days_fetched={out.get('ccxt_days_fetched', 0)}  minutes_written={out.get('minutes_written', 0)}  notes={out.get('notes', [])}")
        update_progress(stage="running", last_result=out)
        try:
            _refresh_inventory_coin("bybit", "1m", coin)
        except Exception:
            pass

    _append_to_job_log(job_id, f"job finished  duration={int(time.time()-started_ts)}s")


def _run_okx_best_1m(job_path: Path, payload: dict[str, Any]) -> None:
    started_ts = time.time()
    job_id = job_path.stem
    coins = payload.get("coins")
    coins = coins if isinstance(coins, list) else []
    coins = [str(c).strip().upper() for c in coins if str(c).strip()]

    if not coins:
        raise ValueError("No coins in job")

    end_day = str(payload.get("end_day") or "").strip()
    if not end_day:
        end_day = datetime.utcnow().strftime("%Y%m%d")
    start_day = str(payload.get("start_day") or "").strip()
    refetch = bool(payload.get("refetch") or False)

    total_steps = max(1, len(coins))
    completed_count = 0
    started_count = 0
    pipeline_workers = min(len(coins), OKX_BEST_1M_PIPELINE_WORKERS)
    try:
        requested_workers = int(payload.get("pipeline_workers") or 0)
        if requested_workers > 0:
            pipeline_workers = min(len(coins), max(1, min(4, requested_workers)))
    except Exception:
        pass

    _init_job_log(job_id)
    _append_to_job_log(job_id, f"job started  coins={coins}  end_day={end_day}  start_day={start_day or 'inception'}  refetch={refetch}")
    _append_to_job_log(job_id, f"pipeline  workers={pipeline_workers}  shared_rest_rate={_OKX_REST_RATE_PER_SECOND}/s")

    progress_lock = threading.Lock()
    log_lock = threading.Lock()
    state_lock = threading.Lock()
    ready_event = threading.Event()
    cancel_event = threading.Event()
    advanced_by_step: dict[int, bool] = {}
    rest_limiter = _OkxRateLimiter(_OKX_REST_RATE_PER_SECOND)
    advance_stages = {"archive_index", "archive_download", "archive_bucket", "archive_write", "repair", "rest_recent"}

    def append_job_log(line: str) -> None:
        with log_lock:
            _append_to_job_log(job_id, line)

    def update_progress(**kw):
        def mut(o):
            pr = o.get("progress")
            pr = pr if isinstance(pr, dict) else {}
            pr.update(kw)
            pr["step"] = int(completed_count)
            pr["total"] = int(total_steps)
            pr["pipeline_workers"] = int(pipeline_workers)
            pr["mode"] = "okx_best_1m"
            o["progress"] = pr
        with progress_lock:
            update_job_file(job_path, mutate=mut)

    update_progress(stage="starting", last_result={"days_checked": 0, "minutes_written": 0, "duration_s": 0})

    def _job_stop_check() -> bool:
        return bool(_STOP or cancel_event.is_set() or _is_cancel_requested(job_path))

    def run_coin(step_no: int, coin: str) -> dict[str, Any]:
        if _STOP:
            raise RuntimeError("Worker stopping")
        if _is_cancel_requested(job_path) or cancel_event.is_set():
            raise RuntimeError("cancelled")
        append_job_log(f"[{step_no}/{total_steps}] {coin}  starting")
        update_progress(stage="running", coin=coin, chunk_done=0, chunk_total=0,
                        last_result={"days_checked": 0, "minutes_written": 0,
                                     "duration_s": int(max(0, time.time() - started_ts))})

        last_chunk_update = 0.0
        last_logged_stage = ""
        last_log_ts2: list[float] = [0.0]
        advanced = False

        def progress_cb(snap: dict[str, Any], _coin=coin) -> None:
            nonlocal advanced, last_chunk_update, last_logged_stage
            now = time.time()
            stage = str(snap.get("stage") or "running")
            if not advanced and stage in advance_stages:
                advanced = True
                with state_lock:
                    advanced_by_step[int(step_no)] = True
                ready_event.set()
            if stage != last_logged_stage or now - last_log_ts2[0] >= 60.0:
                last_logged_stage = stage
                last_log_ts2[0] = now
                extra = ""
                if snap.get("day"):
                    extra = f"  day={snap['day']}"
                elif snap.get("month_key"):
                    extra = f"  month={snap['month_key']}"
                elif snap.get("first_archive"):
                    extra = f"  first_archive={snap['first_archive']}"
                done = snap.get("done")
                total = snap.get("total_days") or snap.get("planned")
                if done is not None and total is not None:
                    extra += f"  {done}/{total}"
                append_job_log(f"  {_coin}  stage={stage}{extra}")
            if now - last_chunk_update < 0.5:
                return
            last_chunk_update = now
            kw: dict[str, Any] = {"stage": stage}
            if snap.get("day"):
                kw["day"] = str(snap["day"])
            if snap.get("month_key"):
                kw["month_key"] = str(snap["month_key"])
            done = snap.get("done")
            if done is not None:
                total_days = snap.get("total_days") or snap.get("planned")
                kw["chunk_done"] = int(done)
                kw["chunk_total"] = int(total_days) if total_days else int(total_steps * 100)
            if any(k in snap for k in ("days_checked", "minutes_written", "rest_minutes_fetched", "repair_minutes_fetched")):
                kw["last_result"] = {
                    "days_checked": int(snap.get("days_checked") or 0),
                    "rest_minutes_fetched": int(snap.get("rest_minutes_fetched") or 0),
                    "repair_minutes_fetched": int(snap.get("repair_minutes_fetched") or 0),
                    "minutes_written": int(snap.get("minutes_written") or 0),
                    "duration_s": int(max(0, time.time() - started_ts)),
                }
            update_progress(**kw)

        res = improve_best_okx_1m_for_coin(
            coin=coin,
            end_date=end_day,
            start_date_override=start_day or None,
            refetch=refetch,
            progress_cb=progress_cb,
            stop_check=_job_stop_check,
            rest_limiter=rest_limiter,
        )
        out = res.to_dict()
        if isinstance(out, dict):
            out["duration_s"] = int(max(0, time.time() - started_ts))
        return out

    executor = ThreadPoolExecutor(max_workers=max(1, int(pipeline_workers)))
    active: dict[Any, tuple[int, str]] = {}

    def submit_next() -> None:
        nonlocal started_count
        if started_count >= len(coins):
            return
        if _STOP:
            raise RuntimeError("Worker stopping")
        if _is_cancel_requested(job_path) or cancel_event.is_set():
            raise RuntimeError("cancelled")
        started_count += 1
        coin = coins[started_count - 1]
        future = executor.submit(run_coin, started_count, coin)
        active[future] = (started_count, coin)

    try:
        submit_next()
        while active:
            done, _pending = wait(tuple(active.keys()), timeout=0.5, return_when=FIRST_COMPLETED)
            if _STOP or _is_cancel_requested(job_path):
                cancel_event.set()
                for future in active:
                    future.cancel()
                raise RuntimeError("cancelled" if _is_cancel_requested(job_path) else "Worker stopping")

            for future in done:
                step_no, coin = active.pop(future)
                try:
                    out = future.result()
                except RuntimeError as exc:
                    cancel_event.set()
                    append_job_log(f"  {coin}  ERROR {exc}")
                    for other in active:
                        other.cancel()
                    if _is_cancel_requested(job_path):
                        raise RuntimeError("cancelled") from exc
                    raise
                except Exception as exc:
                    cancel_event.set()
                    append_job_log(f"  {coin}  ERROR {exc}")
                    for other in active:
                        other.cancel()
                    raise

                completed_count += 1
                append_exchange_download_log("okx", f"[INFO] [okx_best_1m_job] {coin} {out}")
                append_job_log(f"  {coin}  done  days_checked={out.get('days_checked', 0)}  minutes_written={out.get('minutes_written', 0)}  notes={out.get('notes', [])}")
                update_progress(stage="running", coin=coin, last_result=out)
                with state_lock:
                    was_advanced = bool(advanced_by_step.pop(int(step_no), False))
                if not active or not was_advanced:
                    ready_event.set()
                try:
                    _refresh_inventory_coin("okx", "1m", _okx_storage_coin_dir(coin))
                except Exception:
                    pass

            if ready_event.is_set() and len(active) < pipeline_workers and started_count < len(coins):
                ready_event.clear()
                submit_next()
    finally:
        cancel = bool(cancel_event.is_set() or _STOP or _is_cancel_requested(job_path))
        executor.shutdown(wait=True, cancel_futures=cancel)

    update_progress(stage="running", last_result={"duration_s": int(max(0, time.time() - started_ts))})
    _append_to_job_log(job_id, f"job finished  duration={int(time.time()-started_ts)}s")


def _run_cache_sweep_thread(interval_s: float = 600.0) -> None:
    """Background daemon thread: periodically sweep inventory cache for external file changes.

    Checks all cached (exchange, dataset, coin) entries against current dir mtimes.
    Refreshes coins whose files changed (e.g. manually deleted/moved), removes
    entries whose directories no longer exist.
    Runs every `interval_s` seconds (default: 10 minutes).
    """
    while not _STOP:
        try:
            result = _sweep_cache_mtimes()
            if result["refreshed"] or result["deleted"]:
                _job_log(
                    f"[cache-sweep] refreshed={result['refreshed']}  "
                    f"deleted={result['deleted']}  unchanged={result['unchanged']}"
                )
        except Exception as e:
            _job_log(f"[cache-sweep] error: {e}")
        # Sleep in small increments so _STOP is checked promptly
        deadline = time.monotonic() + interval_s
        while not _STOP and time.monotonic() < deadline:
            time.sleep(5.0)


def main() -> int:
    ensure_task_dirs()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    pid = os.getpid()
    write_worker_pid(pid)
    _job_log(f"worker started pid={pid}")

    # Start background cache sweep thread (checks external file changes every 10 min)
    _sweep_t = threading.Thread(
        target=_run_cache_sweep_thread,
        kwargs={"interval_s": 600.0},
        daemon=True,
        name="cache-sweep",
    )
    _sweep_t.start()

    # Keep one regular FIFO slot per job type. A manual run request may open one
    # additional same-type slot so one extra pending job can run in parallel.
    active_threads: dict[str, dict[str, Any]] = {}
    threads_lock = threading.Lock()

    def _run_job_thread(job_run: Path, job_id: str) -> None:
        """Thread target: run one job, handle requeue/fail, then unregister."""
        try:
            _run_job(job_run)
        except Exception as e:
            # Graceful worker stop: requeue so the job resumes after restart.
            if _STOP and job_run.exists() and not _is_cancel_requested(job_run):
                try:
                    update_job_file(
                        job_run,
                        mutate=lambda o: o.update(
                            {"status": "pending", "error": "worker stopped; requeued"}
                        ),
                    )
                    move_job_file(job_run, "pending")
                    _job_log(f"worker stopping; requeued job {job_run.name}", level="WARNING")
                    return
                except Exception:
                    pass
            _job_log(f"fatal in job runner: {e}")
            try:
                if job_run.exists():
                    update_job_file(job_run, mutate=lambda o: o.update({"status": "failed", "error": str(e)}))
                    move_job_file(job_run, "failed")
            except Exception:
                pass
        finally:
            with threads_lock:
                active_threads.pop(job_id, None)

    try:
        # On startup ALL running/ files are stale (worker was killed or crashed).
        # max_age_s=0 requeues every file regardless of mtime — even jobs that were
        # actively updating their progress file seconds before the crash.
        _requeue_stale_running_jobs(max_age_s=0)

        consecutive_errors = 0
        while not _STOP:
            try:
                # Refresh PID file periodically.
                try:
                    write_worker_pid(os.getpid())
                except Exception:
                    pass

                running_dir = get_task_state_dir("running")
                running_counts: dict[str, dict[str, int]] = {}
                for running_path in running_dir.glob("*.json"):
                    running_obj = _load_job(running_path)
                    if not running_obj:
                        continue
                    running_type = str(running_obj.get("type") or "").strip()
                    if not running_type:
                        continue
                    bucket = running_counts.setdefault(running_type, {"running": 0, "manual": 0})
                    bucket["running"] += 1
                    if bool(running_obj.get("manual_parallel")):
                        bucket["manual"] += 1

                pending_dir = get_task_state_dir("pending")
                jobs: list[tuple[Path, dict[str, Any], bool, Any]] = []
                for job_src in pending_dir.glob("*.json"):
                    obj = _load_job(job_src)
                    if not obj:
                        continue
                    manual_run = bool(obj.get("run_requested")) and str(obj.get("status") or "").strip().lower() == "pending"
                    manual_run_ts = int(obj.get("run_requested_ts") or 0) if manual_run else 0
                    sort_key: Any = manual_run_ts if manual_run else job_src.name
                    jobs.append((job_src, obj, manual_run, sort_key))
                jobs.sort(key=lambda item: (0 if item[2] else 1, item[3]))

                if not jobs:
                    time.sleep(2.0)
                    consecutive_errors = 0
                    continue

                started_any = False
                for job_src, obj, manual_run, _sort_key in jobs:
                    if _STOP:
                        break
                    jtype = str(obj.get("type") or "").strip()
                    if not jtype:
                        continue
                    with threads_lock:
                        type_counts = running_counts.get(jtype, {"running": 0, "manual": 0})
                        same_type_running = int(type_counts.get("running") or 0)
                        same_type_manual = int(type_counts.get("manual") or 0)

                        if manual_run:
                            if same_type_running >= 2 or same_type_manual >= 1:
                                continue
                        elif same_type_running >= 1:
                            # Regular queue keeps a single FIFO slot per job type.
                            continue

                        try:
                            job_run = move_job_file(job_src, "running")
                        except Exception:
                            continue
                        job_id = job_run.stem
                        t = threading.Thread(
                            target=_run_job_thread,
                            args=(job_run, job_id),
                            daemon=True,
                            name=f"job-{jtype}-{job_id[:6]}",
                        )
                        active_threads[job_id] = {
                            "thread": t,
                            "type": jtype,
                            "manual_parallel": manual_run,
                        }
                        running_counts[jtype] = {
                            "running": same_type_running + 1,
                            "manual": same_type_manual + (1 if manual_run else 0),
                        }
                    t.start()
                    _job_log(
                        f"started job {job_run.name} type={jtype}"
                        + (" manual_parallel=1" if manual_run else "")
                    )
                    started_any = True

                if not started_any:
                    time.sleep(1.0)
                consecutive_errors = 0

            except Exception as loop_err:
                consecutive_errors += 1
                _job_log(f"unexpected error in main loop (#{consecutive_errors}): {loop_err}")
                if consecutive_errors >= 10:
                    _job_log("too many consecutive errors, worker exiting")
                    return 1
                time.sleep(min(5.0 * consecutive_errors, 30.0))

        # Wait for running threads to finish (each will requeue its job on _STOP).
        _job_log("worker stopping; waiting for active jobs...")
        for meta in list(active_threads.values()):
            thread = meta.get("thread") if isinstance(meta, dict) else meta
            if isinstance(thread, threading.Thread):
                thread.join(timeout=30.0)
        _job_log("worker stopping")
        return 0
    finally:
        clear_worker_pid()


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--run-job":
        raise SystemExit(run_single_job_file(sys.argv[2]))
    raise SystemExit(main())
