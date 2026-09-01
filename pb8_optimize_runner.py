"""Detached PB8 optimize runner with launch handshake and durable exit state."""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
import threading
import time
from pathlib import Path

import psutil

from master_update_lock import MasterUpdateBusyError, acquire_master_runtime_lock
from secure_files import atomic_write_private_text
from sweep_cycles import SWEEP_PLAN_FILENAME, validate_sweep_plan


EVALUATION_PROGRESS_FILENAME = ".pbgui_optimize_progress.json"


def _install_evaluation_progress_publisher(optimize_module):
    """Publish PB8's exact in-memory evaluation count without changing PB8 source."""
    recorder_class = getattr(optimize_module, "ResultRecorder", None)
    if recorder_class is None or getattr(recorder_class, "_pbgui_progress_patched", False):
        return lambda: None
    instances = []
    original_init = recorder_class.__init__
    original_record = recorder_class.record

    def publish(instance, force=False):
        now = time.monotonic()
        if not force and now - float(getattr(instance, "_pbgui_progress_at", 0.0)) < 1.0:
            return
        store = getattr(instance, "store", None)
        result_dir = Path(str(getattr(store, "directory", "")))
        evaluations = int(getattr(store, "n_iters", 0) or 0)
        if not result_dir.is_dir() or evaluations < 0:
            return
        payload = {
            "contract_version": 1,
            "evaluations": evaluations,
            "updated_at": time.time(),
        }
        results_file = getattr(instance, "results_file", None)
        try:
            if results_file is not None:
                payload["all_results_size"] = int(results_file.tell())
            atomic_write_private_text(
                result_dir / EVALUATION_PROGRESS_FILENAME,
                json.dumps(payload, indent=4) + "\n",
            )
            instance._pbgui_progress_at = now
        except (OSError, ValueError):
            return

    def patched_init(instance, *args, **kwargs):
        original_init(instance, *args, **kwargs)
        instances.append(instance)

    def patched_record(instance, data):
        result = original_record(instance, data)
        publish(instance)
        return result

    recorder_class.__init__ = patched_init
    recorder_class.record = patched_record
    recorder_class._pbgui_progress_patched = True
    return lambda: [publish(instance, force=True) for instance in instances]


def _persist_open_sweep_plan(pb8_dir: Path, plan: dict) -> bool:
    """Persist a validated plan beside this runner's open PB8 result stream."""
    results_root = (pb8_dir / "optimize_results").resolve(strict=False)
    try:
        open_files = psutil.Process(os.getpid()).open_files()
    except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
        return False
    for opened in open_files:
        try:
            path = Path(str(opened.path)).resolve(strict=False)
            relative = path.relative_to(results_root)
        except (AttributeError, OSError, ValueError):
            continue
        if len(relative.parts) != 2 or relative.parts[-1] != "all_results.bin":
            continue
        result_dir = path.parent
        if not result_dir.is_dir() or result_dir.is_symlink():
            continue
        target = result_dir / SWEEP_PLAN_FILENAME
        atomic_write_private_text(target, json.dumps(plan, indent=4) + "\n")
        return True
    return False


def _monitor_sweep_plan(pb8_dir: Path, plan: dict, stop_event: threading.Event) -> None:
    """Watch only this runner until its PB8 result stream is opened."""
    while not stop_event.is_set():
        if _persist_open_sweep_plan(pb8_dir, plan):
            return
        stop_event.wait(0.5)


def _optimizer_argv(cli_path: str, config_path: str, options: dict) -> list[str]:
    argv = [cli_path, config_path]
    mode = str(options.get("mode") or "fresh")
    source = str(options.get("source") or "")
    if mode == "checkpoint_resume":
        argv.extend(["--resume", source])
    elif mode == "pareto_seed":
        argv.extend(["--start", source])
    elif mode != "fresh":
        raise ValueError(f"Unsupported optimize launch mode: {mode}")
    fine_tune = options.get("fine_tune_params")
    if isinstance(fine_tune, list):
        fine_tune = ",".join(str(item).strip() for item in fine_tune if str(item).strip())
    if str(fine_tune or "").strip():
        argv.extend(["--fine-tune-params", str(fine_tune).strip()])
    polish_pct = options.get("polish_percentage")
    if polish_pct is not None:
        argv.extend(["--polish-pct", str(polish_pct)])
        argv.extend(["--polish-bounds-mode", str(options.get("polish_bounds_mode") or "clamp")])
    return argv


def main(argv: list[str] | None = None) -> int:
    """Run one PB8 optimizer and atomically publish its final return code."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 8 or arguments[0] != "optimize":
        raise SystemExit(
            "usage: pb8_optimize_runner.py optimize STATE OWNERSHIP READY CLI PB8_DIR CONFIG OPTIONS"
        )
    _operation, state_path, ownership_path, ready_path, cli_path, pb8_dir, config_path, options_path = arguments
    started_at = time.time()
    returncode = 1
    error = ""
    sweep_stop = threading.Event()
    sweep_thread = None
    sweep_plan = None
    runtime_pb8_dir = None
    publish_final_progress = lambda: None
    try:
        ownership = {"pid": os.getpid(), "create_time": psutil.Process(os.getpid()).create_time()}
        atomic_write_private_text(Path(ownership_path), json.dumps(ownership, indent=4) + "\n")

        pbgui_dir = Path(__file__).resolve().parent
        invalid_marker = pbgui_dir / "data" / "locks" / "pb8-runtime-invalid"
        if invalid_marker.exists() or invalid_marker.is_symlink():
            raise RuntimeError("PB8 installation or update is incomplete")
        try:
            runtime_lease = acquire_master_runtime_lock(pbgui_dir)
        except MasterUpdateBusyError as exc:
            raise RuntimeError(str(exc)) from exc
        previous_argv = sys.argv[:]
        try:
            if invalid_marker.exists() or invalid_marker.is_symlink():
                raise RuntimeError("PB8 installation or update is incomplete")
            options = json.loads(Path(options_path).read_text(encoding="utf-8"))
            if not isinstance(options, dict):
                raise TypeError("Optimize launch options must be an object")
            sweep_plan = validate_sweep_plan(options.get("pbgui_sweep_plan"))
            runtime_pb8_dir = Path(pb8_dir).resolve(strict=False)
            sys.argv = _optimizer_argv(cli_path, config_path, options)
            optimize_module = importlib.import_module("optimize")
            publish_final_progress = _install_evaluation_progress_publisher(optimize_module)
            atomic_write_private_text(Path(ready_path), f"{os.getpid()}\n")
        finally:
            runtime_lease.release()
        try:
            if sweep_plan is not None and runtime_pb8_dir is not None:
                sweep_thread = threading.Thread(
                    target=_monitor_sweep_plan,
                    args=(runtime_pb8_dir, sweep_plan, sweep_stop),
                    name="pb8-sweep-plan-monitor",
                    daemon=True,
                )
                sweep_thread.start()
            try:
                result = asyncio.run(optimize_module.main())
                returncode = int(result) if isinstance(result, int) else 0
            except SystemExit as exc:
                returncode = int(exc.code) if isinstance(exc.code, int) else (0 if exc.code is None else 1)
        finally:
            sys.argv = previous_argv
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    finally:
        publish_final_progress()
        sweep_stop.set()
        if sweep_thread is not None:
            sweep_thread.join(timeout=2)
    payload = {"started_at": started_at, "completed_at": time.time(), "returncode": returncode}
    if error:
        payload["error"] = error
    atomic_write_private_text(Path(state_path), json.dumps(payload, indent=4) + "\n")
    return returncode


if __name__ == "__main__":
    raise SystemExit(main())
