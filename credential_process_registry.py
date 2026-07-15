"""Process-aware protocol capability registry for credential rolling upgrades."""

from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Iterable, Mapping

import psutil

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory, secure_private_file


SERVICE = "CredentialProcessRegistry"
CREDENTIAL_PROTOCOL_VERSION = 2
CAPABILITY_GENERATION = 1
HEARTBEAT_INTERVAL_SECONDS = 5.0
HEARTBEAT_MAX_AGE_SECONDS = 30.0
MAX_REGISTRY_ENTRIES = 128

RELEVANT_PROCESS_SCRIPTS = {
    "PBApiServer.py": "PBApiServer",
    "PBCluster.py": "PBCluster",
    "PBCoinData.py": "PBCoinData",
    "PBRun.py": "PBRun",
    "task_worker.py": "Market Data worker",
    "tradfi_sync.py": "TradFi Sync",
    "hyperliquid_best_1m.py": "Market Data TradFi job",
    "reprocess_tradfi_splits.py": "TradFi split job",
    "monitor_agent.py": "PBMonitorAgent",
}


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _code_serial(root: Path) -> str:
    serial_path = root / "api" / "serial.txt"
    try:
        value = serial_path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""
    return value[:32]


def _registry_paths(root: Path) -> tuple[Path, Path]:
    runtime_root = root / "data" / "credentials" / "runtime"
    ensure_private_directory(root / "data" / "credentials")
    ensure_private_directory(runtime_root)
    return runtime_root / "capabilities.json", runtime_root / "capabilities"


def _empty_registry() -> dict[str, Any]:
    return {"version": 1, "entries": {}}


def _read_registry(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _empty_registry()
    secure_private_file(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _empty_registry()
    if not isinstance(value, dict) or value.get("version") != 1 or not isinstance(value.get("entries"), dict):
        return _empty_registry()
    return value


def _write_registry(path: Path, registry: Mapping[str, Any]) -> None:
    atomic_write_private_text(path, json.dumps(dict(registry), indent=4, sort_keys=True) + "\n")


def _process_matches(pid: int, create_time: float) -> bool:
    try:
        return abs(float(psutil.Process(pid).create_time()) - float(create_time)) < 0.01
    except (psutil.Error, OSError, ValueError):
        return False


def _prune_entries(entries: Mapping[str, Any], now: float) -> dict[str, Any]:
    kept: list[tuple[str, dict[str, Any]]] = []
    for key, raw in entries.items():
        if not isinstance(raw, dict):
            continue
        try:
            pid = int(raw["pid"])
            create_time = float(raw["create_time"])
            heartbeat = float(raw["heartbeat_monotonic"])
        except (KeyError, TypeError, ValueError):
            continue
        if not _process_matches(pid, create_time) or now - heartbeat > HEARTBEAT_MAX_AGE_SECONDS * 4:
            continue
        kept.append((str(key), dict(raw)))
    kept.sort(key=lambda item: float(item[1].get("heartbeat_monotonic") or 0), reverse=True)
    return dict(kept[:MAX_REGISTRY_ENTRIES])


def register_process_capability(
    root: Path | str,
    service: str,
    *,
    code_serial: str | None = None,
) -> dict[str, Any]:
    """Register or refresh the current independently running service."""
    root = Path(os.path.abspath(Path(root).expanduser()))
    path, lock_target = _registry_paths(root)
    pid = os.getpid()
    create_time = float(psutil.Process(pid).create_time())
    now = time.monotonic()
    record = {
        "pid": pid,
        "create_time": create_time,
        "service": str(service)[:96],
        "credential_protocol_version": CREDENTIAL_PROTOCOL_VERSION,
        "code_serial": _code_serial(root) if code_serial is None else str(code_serial)[:32],
        "capability_generation": CAPABILITY_GENERATION,
        "heartbeat_monotonic": now,
        "updated_at": _timestamp(),
    }
    key = f"{pid}:{create_time:.6f}"
    with advisory_file_lock(lock_target):
        registry = _read_registry(path)
        entries = _prune_entries(registry["entries"], now)
        entries[key] = record
        registry["entries"] = entries
        _write_registry(path, registry)
    return dict(record)


def unregister_process_capability(root: Path | str) -> None:
    """Remove the current process entry on orderly shutdown."""
    root = Path(os.path.abspath(Path(root).expanduser()))
    path, lock_target = _registry_paths(root)
    pid = os.getpid()
    try:
        create_time = float(psutil.Process(pid).create_time())
    except psutil.Error:
        return
    key = f"{pid}:{create_time:.6f}"
    with advisory_file_lock(lock_target):
        registry = _read_registry(path)
        registry["entries"] = _prune_entries(registry["entries"], time.monotonic())
        registry["entries"].pop(key, None)
        _write_registry(path, registry)


class ProcessCapabilityHeartbeat(AbstractContextManager["ProcessCapabilityHeartbeat"]):
    """Keep one process capability current until the service exits."""

    def __init__(self, root: Path | str, service: str) -> None:
        self.root = Path(root)
        self.service = str(service)
        self.code_serial = _code_serial(Path(os.path.abspath(self.root.expanduser())))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "ProcessCapabilityHeartbeat":
        register_process_capability(self.root, self.service, code_serial=self.code_serial)
        self._thread = threading.Thread(target=self._run, name="credential-capability", daemon=True)
        self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.wait(HEARTBEAT_INTERVAL_SECONDS):
            try:
                register_process_capability(self.root, self.service, code_serial=self.code_serial)
            except Exception:
                continue

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=HEARTBEAT_INTERVAL_SECONDS + 1)
        try:
            unregister_process_capability(self.root)
        except Exception:
            pass

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.close()


def _service_for_process(
    root: Path,
    command: Iterable[str],
    *,
    cwd: Path | str | None = None,
) -> str:
    args = [str(item) for item in command if item]
    app_dir = Path(os.path.abspath(Path(cwd or "/").expanduser()))
    for index, argument in enumerate(args):
        if argument == "--app-dir" and index + 1 < len(args):
            app_dir = Path(os.path.abspath(Path(args[index + 1]).expanduser()))
        elif argument.startswith("--app-dir="):
            app_dir = Path(os.path.abspath(Path(argument.split("=", 1)[1]).expanduser()))
    if "PBApiServer:app" in args and app_dir == root and (root / "PBApiServer.py").is_file():
        return "PBApiServer"
    for index, argument in enumerate(args):
        candidate = Path(argument)
        if candidate.name in RELEVANT_PROCESS_SCRIPTS:
            absolute = candidate if candidate.is_absolute() else root / candidate
            if Path(os.path.abspath(absolute)) == root / candidate.name:
                return RELEVANT_PROCESS_SCRIPTS[candidate.name]
        if argument == "-m" and index + 1 < len(args):
            module = args[index + 1].removesuffix(".py") + ".py"
            if module in RELEVANT_PROCESS_SCRIPTS and (root / module).is_file():
                return RELEVANT_PROCESS_SCRIPTS[module]
    return ""


def running_relevant_processes(root: Path | str) -> list[dict[str, Any]]:
    """Return value-free metadata for live PBGui credential consumers below root."""
    root = Path(os.path.abspath(Path(root).expanduser()))
    result: list[dict[str, Any]] = []
    for process in psutil.process_iter(["pid", "create_time", "cmdline", "cwd"]):
        try:
            service = _service_for_process(
                root,
                process.info.get("cmdline") or [],
                cwd=process.info.get("cwd"),
            )
            if service:
                result.append({
                    "pid": int(process.info["pid"]),
                    "create_time": float(process.info["create_time"]),
                    "service": service,
                })
        except (psutil.Error, OSError, TypeError, ValueError):
            continue
    return result


def process_barrier_readiness(
    root: Path | str,
    *,
    processes: Iterable[Mapping[str, Any]] | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Require each live relevant PID/start time to have a fresh matching v2 entry."""
    root = Path(os.path.abspath(Path(root).expanduser()))
    path, lock_target = _registry_paths(root)
    checked_at = time.monotonic() if now is None else float(now)
    with advisory_file_lock(lock_target):
        registry = _read_registry(path)
        entries = _prune_entries(registry["entries"], checked_at)
        registry["entries"] = entries
        _write_registry(path, registry)
    live = list(processes) if processes is not None else running_relevant_processes(root)
    serial = _code_serial(root)
    ready_by_service: dict[str, dict[str, Any]] = {}
    blocked_services: set[str] = set()
    for process in live:
        pid = int(process["pid"])
        create_time = float(process["create_time"])
        service = str(process["service"])
        record = entries.get(f"{pid}:{create_time:.6f}")
        current = (
            isinstance(record, dict)
            and int(record.get("pid") or 0) == pid
            and abs(float(record.get("create_time") or 0) - create_time) < 0.01
            and int(record.get("credential_protocol_version") or 0) == CREDENTIAL_PROTOCOL_VERSION
            and int(record.get("capability_generation") or 0) == CAPABILITY_GENERATION
            and str(record.get("service") or "") == service
            and str(record.get("code_serial") or "") == serial
            and checked_at - float(record.get("heartbeat_monotonic") or 0) <= HEARTBEAT_MAX_AGE_SECONDS
        )
        if not current:
            blocked_services.add(service)
            continue
        ready_by_service[service] = {
            "service": service,
            "credential_protocol_version": CREDENTIAL_PROTOCOL_VERSION,
            "code_serial": serial,
            "capability_generation": CAPABILITY_GENERATION,
        }
    ready = [ready_by_service[service] for service in sorted(ready_by_service)]
    return {
        "ready": not blocked_services,
        "services": ready,
        "waiting_services": sorted(blocked_services),
    }


__all__ = [
    "CAPABILITY_GENERATION",
    "CREDENTIAL_PROTOCOL_VERSION",
    "ProcessCapabilityHeartbeat",
    "process_barrier_readiness",
    "register_process_capability",
    "running_relevant_processes",
    "unregister_process_capability",
]
