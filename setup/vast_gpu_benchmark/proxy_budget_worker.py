"""Job-scoped proxy watchdog around the immutable image's existing worker.

No provider connection, credentials, optimizer configuration changes or PB8
patches. The native wrapper remains responsible for graceful process shutdown.
"""

from __future__ import annotations

import fcntl
import importlib.util
import json
import os
from pathlib import Path
import re
import threading
import time

SERVICE = 'VastProxyBudget'
PROGRESS = re.compile(
    rb'GPU optimizer progress\s*\|[^\n]*\bevolution_proxy_completed_run=(\d+)(?=\s|$)'
    rb'|GPU optimize\s*\|\s*gen=\d+\s+proxy=(\d+)(?=\s|$)')


def record(root: Path, **values) -> None:
    """Publish non-secret guard evidence privately and atomically."""
    target = root / 'proxy-budget-state.json'
    temporary = root / 'proxy-budget-state.json.tmp'
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(dict(pid=os.getpid(), **values), stream, indent=4, allow_nan=False)
    os.replace(temporary, target)


def request_stop(root: Path) -> None:
    """Request the existing worker's graceful stop, scoped to this job only."""
    fd = os.open(root / 'stop', os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    os.close(fd)


def completed_proxies(root: Path) -> int:
    """Read bounded native completed-work evidence, excluding seeds/dispatches."""
    path = root / 'output/optimizer.log'
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return 0
    with os.fdopen(fd, 'rb') as stream:
        size = os.fstat(stream.fileno()).st_size
        stream.seek(max(0, size - 1024 * 1024))
        data = stream.read(1024 * 1024)
    return max((int(match.group(1) or match.group(2)) for match in PROGRESS.finditer(data)), default=0)


def watch(root: Path, limit: int, done: threading.Event, *, deadline_path: Path) -> None:
    """Stop at the first locally observed completed batch; never contact PBGui."""
    maximum = 0
    waiting_until = time.monotonic() + 120
    try:
        while not done.is_set():
            if (root / 'finished.json').exists():
                return
            deadline = json.loads(deadline_path.read_text())['deadline']
            if type(deadline) not in (int, float) or not 0 < deadline < float('inf'):
                raise ValueError('Invalid rental deadline')
            maximum = max(maximum, completed_proxies(root))
            if maximum >= limit or time.time() >= deadline - 180:
                request_stop(root)
                record(root, limit=limit, observed=maximum, stop_requested_at=time.time(),
                       reason='proxy_limit_reached' if maximum >= limit else 'rental_deadline')
                return
            if not (root / 'started.json').exists() and time.monotonic() >= waiting_until:
                raise RuntimeError('Native worker did not start')
            done.wait(1)
    except Exception as exc:
        # A broken watchdog must not leave an unbounded paid optimizer running.
        request_stop(root)
        record(root, limit=limit, observed=maximum, reason='watchdog_error',
               error=type(exc).__name__, stop_requested_at=time.time())


def run(root: Path, limit: int, *, attach: bool = False,
        worker_path: Path = Path('/work/pbgui/worker.py'),
        deadline_path: Path = Path('/work/pbgui/guard.json')) -> None:
    """Own one guard per job; join the monitor when the native wrapper exits."""
    if type(limit) is not int or limit <= 0:
        raise ValueError('Invalid proxy limit')
    fd = os.open(root / 'proxy-budget.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        if (root / 'finished.json').exists():
            return
        done = threading.Event()
        record(root, limit=limit, status='watching', attached=attach, started_at=time.time())
        if attach:
            watch(root, limit, done, deadline_path=deadline_path)
            return
        os.environ['PBGUI_WORKDIR'] = str(root)
        spec = importlib.util.spec_from_file_location('pbgui_native_worker', worker_path)
        worker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(worker)
        monitor = threading.Thread(target=watch, args=(root, limit, done),
                                   kwargs={'deadline_path': deadline_path}, name='proxy-budget')
        monitor.start()
        try:
            worker.run()
        except Exception as exc:
            worker.write_record(root / 'finished.json', dict(exit_code=1, cancelled=False,
                                error=type(exc).__name__, finished_at=time.time()))
            raise
        finally:
            done.set()
            monitor.join()


def main() -> None:
    """Accept only a canonical isolated job root and a positive controller limit."""
    import sys
    os.umask(0o077)
    root = Path(sys.argv[1])
    if (not re.fullmatch(r'/work/pbgui/jobs/[0-9a-f]{32}', str(root))
            or any(path.is_symlink() for path in (root, *root.parents))):
        raise ValueError('Invalid job root')
    if len(sys.argv) not in (3, 4) or (len(sys.argv) == 4 and sys.argv[3] != 'attach'):
        raise ValueError('Invalid watchdog arguments')
    run(root, int(sys.argv[2]), attach=len(sys.argv) == 4)


if __name__ == '__main__':
    main()
