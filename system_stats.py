"""Local system information for the Welcome and Storage pages.

All collectors are synchronous and bounded; callers run them in a thread and
cache the result for a few seconds so polling pages never load the API.
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import psutil

SERVICE = "Storage"

GPU_TTL_S = 60.0
PYTHON_TTL_S = 3600.0

_lock = threading.Lock()
_process_cache: dict[int, psutil.Process] = {}
_gpu_cache: tuple[float, list[dict[str, Any]]] = (0.0, [])
_python_cache: dict[str, tuple[float, str]] = {}
_static_cache: dict[str, Any] = {}
_cpu_sample: tuple[float, Any] | None = None
_cpu_value = 0.0
_cpu_lock = threading.Lock()
CPU_MIN_WINDOW_S = 0.5


def _cpu_total_busy(times: Any) -> tuple[float, float]:
    """Return total and busy CPU time with psutil's own accounting rules."""
    total = sum(times)
    # Guest time is already contained in user/nice on Linux.
    total -= getattr(times, "guest", 0.0) + getattr(times, "guest_nice", 0.0)
    idle = times.idle + getattr(times, "iowait", 0.0)
    return total, total - idle


def cpu_percent() -> float:
    """Return system CPU usage since the previous call, independent of the thread.

    ``psutil.cpu_percent(interval=None)`` keeps its reference sample per thread,
    so calls from changing executor threads mostly measure an empty window.
    """
    global _cpu_sample, _cpu_value
    now = time.monotonic()
    current = psutil.cpu_times()
    with _cpu_lock:
        previous = _cpu_sample
        if previous is not None and now - previous[0] < CPU_MIN_WINDOW_S:
            return _cpu_value
        _cpu_sample = (now, current)
        if previous is None:
            return _cpu_value
        total_now, busy_now = _cpu_total_busy(current)
        total_before, busy_before = _cpu_total_busy(previous[1])
        total = total_now - total_before
        if total > 0:
            _cpu_value = round(max(0.0, min(100.0, 100.0 * (busy_now - busy_before) / total)), 1)
        return _cpu_value


def prime() -> None:
    """Take the first CPU sample so the first real read is meaningful."""
    cpu_percent()


def _cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace").splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or platform.machine()


def _os_name() -> str:
    try:
        for line in Path("/etc/os-release").read_text(encoding="utf-8").splitlines():
            if line.startswith("PRETTY_NAME="):
                return line.split("=", 1)[1].strip().strip('"')
    except OSError:
        pass
    return f"{platform.system()} {platform.release()}"


def _static() -> dict[str, Any]:
    if not _static_cache:
        _static_cache.update({
            "hostname": platform.node(),
            "os": _os_name(),
            "kernel": platform.release(),
            "arch": platform.machine(),
            "cpu_model": _cpu_model(),
            "cpu_physical": psutil.cpu_count(logical=False) or 0,
            "cpu_logical": psutil.cpu_count(logical=True) or 0,
            "python": platform.python_version(),
        })
    return dict(_static_cache)


def collect_live() -> dict[str, Any]:
    """Return CPU, memory, swap, load and uptime values."""
    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()
    try:
        load = [round(value, 2) for value in os.getloadavg()]
    except (AttributeError, OSError):
        load = []
    try:
        freq = psutil.cpu_freq()
        freq_mhz = round(freq.current) if freq else None
    except Exception:
        freq_mhz = None
    boot = psutil.boot_time()
    return {
        **_static(),
        "cpu_percent": cpu_percent(),
        "cpu_freq_mhz": freq_mhz,
        "load": load,
        "memory": {"total": memory.total, "used": memory.total - memory.available,
                   "available": memory.available, "percent": memory.percent},
        "swap": {"total": swap.total, "used": swap.used, "percent": swap.percent},
        "boot_time": boot,
        "uptime_s": max(0, time.time() - boot),
    }


def _process_name(cmdline: list[str], pbgdir: str) -> str | None:
    """Return the PBGui script or module name a Python command line runs, if any."""
    args = cmdline[1:]
    while args and args[0].startswith("-") and args[0] not in {"-m", "-c"}:
        args = args[1:]
    if not args or args[0] == "-c":
        return None
    if args[0] == "-m":
        if len(args) < 2:
            return None
        module = Path(pbgdir, *args[1].split("."))
        return args[1] if module.with_suffix(".py").is_file() or (module / "__init__.py").is_file() else None
    script = args[0]
    if not script.endswith(".py"):
        return None
    path = os.path.realpath(script if os.path.isabs(script) else os.path.join(pbgdir, script))
    return os.path.basename(script)[:-3] if path.startswith(pbgdir + os.sep) else None


def _helper_name(cmdline: list[str]) -> str | None:
    """Name Python multiprocessing helpers that a PBGui process started."""
    joined = " ".join(cmdline[1:4])
    if "multiprocessing.resource_tracker" in joined:
        return "resource tracker"
    if "multiprocessing" in joined:
        return "worker"
    return None


def collect_processes(pbgdir: Path) -> dict[str, Any]:
    """Return PBGui's own processes with resident memory and CPU usage.

    A process belongs to PBGui when it runs a script or module from the PBGui
    folder, or when it is a multiprocessing helper started by such a process.
    The working directory is not used: unrelated tools may run there.
    """
    root = os.path.realpath(pbgdir)
    rows: dict[int, dict[str, Any]] = {}
    for proc in psutil.process_iter(["pid", "ppid", "cmdline", "memory_info"]):
        info = proc.info
        cmdline = info.get("cmdline") or []
        if cmdline:
            rows[int(info["pid"])] = {"proc": proc, "ppid": info.get("ppid"), "cmdline": cmdline,
                                      "memory": info.get("memory_info"), "name": _process_name(cmdline, root)}
    owners = {pid: row["name"] for pid, row in rows.items() if row["name"]}
    changed = True
    while changed:
        changed = False
        for pid, row in rows.items():
            if pid in owners or row["ppid"] not in owners:
                continue
            helper = _helper_name(row["cmdline"])
            if helper:
                owners[pid] = f"{owners[row['ppid']].split(' · ')[0]} · {helper}"
                changed = True
    items = []
    with _lock:
        for pid, name in owners.items():
            row = rows[pid]
            cached = _process_cache.get(pid)
            if cached is None:
                cached = row["proc"]
                _process_cache[pid] = cached
                try:
                    cached.cpu_percent(interval=None)
                except psutil.Error:
                    pass
                cpu = 0.0
            else:
                try:
                    cpu = cached.cpu_percent(interval=None)
                except psutil.Error:
                    cpu = 0.0
            memory = row["memory"]
            items.append({"pid": pid, "name": name, "rss": int(memory.rss) if memory else 0, "cpu": round(cpu, 1)})
        for pid in list(_process_cache):
            if pid not in owners:
                _process_cache.pop(pid, None)
    items.sort(key=lambda item: item["rss"], reverse=True)
    return {"count": len(items), "rss": sum(item["rss"] for item in items), "items": items}


def collect_gpus() -> list[dict[str, Any]]:
    """Return NVIDIA GPUs via nvidia-smi (cached), or an empty list."""
    global _gpu_cache
    stamp, cached = _gpu_cache
    if time.time() - stamp < GPU_TTL_S:
        return cached
    gpus: list[dict[str, Any]] = []
    binary = shutil.which("nvidia-smi")
    if binary:
        try:
            done = subprocess.run(
                [binary, "--query-gpu=name,memory.total,memory.used,utilization.gpu,temperature.gpu",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5, check=False,
            )
            if done.returncode == 0:
                for line in done.stdout.splitlines():
                    parts = [part.strip() for part in line.split(",")]
                    if len(parts) != 5:
                        continue

                    def _num(value: str) -> float | None:
                        try:
                            return float(value)
                        except ValueError:
                            return None

                    gpus.append({"name": parts[0], "memory_total_mb": _num(parts[1]), "memory_used_mb": _num(parts[2]),
                                 "utilization": _num(parts[3]), "temperature": _num(parts[4])})
        except (OSError, subprocess.SubprocessError):
            gpus = []
    _gpu_cache = (time.time(), gpus)
    return gpus


def python_version(interpreter: str) -> str:
    """Return ``Python X.Y.Z`` for a configured interpreter (cached)."""
    if not interpreter:
        return ""
    stamp, cached = _python_cache.get(interpreter, (0.0, ""))
    if time.time() - stamp < PYTHON_TTL_S:
        return cached
    version = ""
    path = Path(interpreter).expanduser()
    if path.is_file() and os.access(path, os.X_OK):
        try:
            done = subprocess.run([str(path), "--version"], capture_output=True, text=True, timeout=5, check=False)
            version = (done.stdout or done.stderr).strip()
        except (OSError, subprocess.SubprocessError):
            version = ""
    _python_cache[interpreter] = (time.time(), version)
    return version
