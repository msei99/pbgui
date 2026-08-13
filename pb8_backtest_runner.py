"""Detached PB8 backtest runner which persists one job's final exit status."""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
import time
from pathlib import Path

import psutil

from master_update_lock import MasterUpdateBusyError, acquire_master_runtime_lock
from secure_files import atomic_write_private_text


def main(argv: list[str] | None = None) -> int:
    """Run one PB8 CLI backtest and atomically publish its return code."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 7 or arguments[0] != "backtest":
        raise SystemExit("usage: pb8_backtest_runner.py backtest STATE OWNERSHIP READY CLI PB8_DIR CONFIG")
    _operation, state_path, ownership_path, ready_path, cli_path, pb8_dir, config_path = arguments
    started_at = time.time()
    returncode = 1
    error = ""
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
            sys.argv = [cli_path, config_path]
            backtest_module = importlib.import_module("backtest")
            atomic_write_private_text(Path(ready_path), f"{os.getpid()}\n")
        finally:
            runtime_lease.release()
        try:
            try:
                result = asyncio.run(backtest_module.main())
                returncode = int(result) if isinstance(result, int) else 0
            except SystemExit as exc:
                returncode = int(exc.code) if isinstance(exc.code, int) else (0 if exc.code is None else 1)
        finally:
            sys.argv = previous_argv
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    payload = {
        "started_at": started_at,
        "completed_at": time.time(),
        "returncode": returncode,
    }
    if error:
        payload["error"] = error
    atomic_write_private_text(Path(state_path), json.dumps(payload, indent=4) + "\n")
    return returncode


if __name__ == "__main__":
    raise SystemExit(main())
