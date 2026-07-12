"""Detached supervisor for one persistent PB7 OHLCV preload process."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def _read_state(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_state(path: Path, updates: dict) -> dict:
    state = _read_state(path)
    state.update(updates)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=4)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_name, 0o600)
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
    return state


def main() -> int:
    """Run one preload child and persist its authoritative exit status."""
    if len(sys.argv) != 2:
        return 2
    state_path = Path(sys.argv[1]).resolve()
    state = _read_state(state_path)
    command = [str(part) for part in state.get("command") or []]
    if not command:
        _write_state(
            state_path,
            {"status": "error", "error": "Missing preload command", "finished_at": int(time.time() * 1000), "returncode": 2},
        )
        return 2

    stop_requested = False

    def _request_stop(_signum, _frame) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, _request_stop)
    if hasattr(signal, "SIGINT"):
        signal.signal(signal.SIGINT, _request_stop)

    log_path = Path(str(state["log_path"]))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PATH"] = os.path.dirname(command[0]) + os.pathsep + env.get("PATH", "")
    try:
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write("$ " + " ".join(command) + "\n")
            handle.flush()
            child = subprocess.Popen(
                command,
                cwd=str(state["cwd"]),
                env=env,
                stdout=handle,
                stderr=subprocess.STDOUT,
            )
            _write_state(
                state_path,
                {
                    "status": "running",
                    "pid": os.getpid(),
                    "child_pid": child.pid,
                    "error": None,
                },
            )
            returncode = child.wait()
        latest = _read_state(state_path)
        stopped = stop_requested or bool(latest.get("stop_requested"))
        status = "stopped" if stopped else ("completed" if returncode == 0 else "error")
        _write_state(
            state_path,
            {
                "status": status,
                "returncode": returncode,
                "finished_at": int(time.time() * 1000),
                "error": None if returncode == 0 or stopped else f"OHLCV preload exited with code {returncode}",
            },
        )
        return returncode
    except Exception as exc:
        _write_state(
            state_path,
            {
                "status": "error",
                "returncode": 1,
                "finished_at": int(time.time() * 1000),
                "error": str(exc),
            },
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
