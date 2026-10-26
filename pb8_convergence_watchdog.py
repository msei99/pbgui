"""Independent bounded safety net for one verified detached optimizer process."""
from __future__ import annotations

import json
import os
from pathlib import Path
import select
import signal
import sys
import time

import psutil

from secure_files import atomic_write_private_text

SERVICE = 'OptimizeV8'


def _identity(pid, created):
    """Never signal a recycled PID."""
    try:
        process = psutil.Process(pid)
        return process if process.create_time() == created else None
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def _wait_done(seconds, *, clock=time.monotonic, reader=sys.stdin.buffer):
    """Only explicit normal completion cancels cleanup; parent death/EOF does not."""
    deadline = clock() + seconds
    while clock() < deadline:
        ready, _, _ = select.select([reader], [], [], max(0, deadline - clock()))
        if ready:
            message = reader.read(4)
            if message == b'done':
                return True
            # Closed pipe means the runner died; avoid spinning on EOF.
            time.sleep(max(0, deadline - clock()))
    return False


def watch(pid, created, forced_path, wait=_wait_done):
    """After 120 seconds allow 10 seconds for TERM, then kill verified survivors."""
    if wait(120):
        return 0
    parent = _identity(pid, created)
    if parent is None:
        return 0
    children = [proc for proc in parent.children(recursive=True) if proc.pid != os.getpid()]
    owned = [(proc.pid, proc.create_time()) for proc in children] + [(pid, created)]
    try:
        atomic_write_private_text(Path(forced_path), json.dumps({
            'returncode': 1, 'error': 'Graceful convergence shutdown timed out; forced termination',
            'shutdown': {'forced': True, 'complete': False}, 'completed_at': time.time(),
        }, indent=4) + '\n')
    except OSError:
        # A full/unwritable disk must not disable the safety net. The nonzero
        # exit below also prevents a late native cleanup from claiming success
        # when this durable marker could not be written.
        pass
    for sig, delay in ((signal.SIGTERM, 10), (signal.SIGKILL, 0)):
        for owned_pid, owned_created in owned:
            process = _identity(owned_pid, owned_created)
            if process is not None:
                try:
                    process.send_signal(sig)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        if delay:
            # Once escalation starts, even a late completion cannot leave children behind.
            wait(delay)
    return 1


if __name__ == '__main__':
    raise SystemExit(watch(int(sys.argv[1]), float(sys.argv[2]), sys.argv[3]))
