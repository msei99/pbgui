"""Bounded, rollback-capable handover of a rental guard; never touch optimizer processes."""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import sys
import tempfile
import time

SERVICE = 'VastWorker'
ROOT = Path('/work/pbgui')
READY_TIMEOUT = 12


def publish(path, value):
    """Atomically publish an owner-only guard record or executable source."""
    if path.is_symlink():
        raise ValueError('Linked guard file')
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            stream.write(value if isinstance(value, str) else json.dumps(value, indent=4))
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def guard_state(root, request):
    """Validate unchanged identity/deadline and a bounded authorized handover window."""
    path = root / 'guard.json'
    if path.is_symlink() or path.stat().st_size > 8192:
        raise ValueError('Invalid guard record')
    state = json.loads(path.read_text())
    if (state.get('job_id') != request['job_id'] or state.get('instance_id') != request['instance_id']
            or state.get('deadline_protocol') not in (1, 2)):
        raise ValueError('Guard identity or protocol mismatch')
    deadline, maximum, hard = state.get('deadline'), state.get('max_deadline'), request['hard_deadline']
    if (any(type(value) not in (int, float) or not math.isfinite(value) for value in (deadline, maximum, hard))
            or deadline != request['expected'] or not time.time() + 600 < deadline <= maximum <= hard
            or hard > time.time() + 86400):
        raise ValueError('Guard deadline changed or handover window is too short')
    return state


def process_environment(pid, root, request):
    """Read only the identified guard's required environment into private memory."""
    process = Path('/proc') / str(pid)
    args = process.joinpath('cmdline').read_bytes().split(b'\0')
    args = [part.decode() for part in args if part]
    allowed = {str(root / 'worker.py'), str(root / 'deadline-guard-v2.py')}
    if len(args) != 3 or args[1] not in allowed or args[2] != 'guard' or not Path(args[0]).name.startswith('python'):
        return None
    allowed_keys = {'CONTAINER_ID', 'CONTAINER_API_KEY', 'PBGUI_JOB_ID'}
    env = {}
    for part in process.joinpath('environ').read_bytes().split(b'\0'):
        key, _, value = part.partition(b'=')
        name = key.decode(errors='replace')
        if name in allowed_keys:
            env[name] = value.decode()
    if (env.get('CONTAINER_ID') != str(request['instance_id'])
            or env.get('PBGUI_JOB_ID') != request['job_id'] or not env.get('CONTAINER_API_KEY')):
        return None
    return env


def find_guard(root, request):
    """Pin exactly one matching guard with a pidfd, preventing PID-reuse signals."""
    matches = []
    try:
        for process in Path('/proc').iterdir():
            if not process.name.isdigit():
                continue
            descriptor = None
            try:
                descriptor = os.pidfd_open(int(process.name))
                env = process_environment(int(process.name), root, request)
                if env and not select.select([descriptor], [], [], 0)[0]:
                    matches.append((int(process.name), descriptor, env))
                    descriptor = None
            except (OSError, UnicodeError):
                continue
            finally:
                if descriptor is not None:
                    os.close(descriptor)
        if len(matches) != 1:
            raise ValueError('Expected exactly one live guard for this rental')
        return matches.pop()
    finally:
        for _, descriptor, _ in matches:
            os.close(descriptor)


def migrate(root, request):
    """Freeze only the old guard, verify its replacement, then retire it; roll back failures."""
    if (not re.fullmatch(r'[a-f0-9]{32}', str(request.get('job_id', '')))
            or type(request.get('instance_id')) is not int or request['instance_id'] <= 0
            or not isinstance(request.get('source'), str)
            or hashlib.sha256(request['source'].encode()).hexdigest() != request.get('sha256')):
        raise ValueError('Invalid migration request')
    if root.is_symlink() or not root.is_dir():
        raise ValueError('Invalid guard directory')
    lock_fd = os.open(root / '.guard-migration.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = guard_state(root, request)
        pid, descriptor, env = find_guard(root, request)
        stopped, committed, child = False, False, None
        archived_request = False
        request_path = root / 'deadline-request.json'
        archive_path = root / '.deadline-request-before-upgrade.json'
        try:
            if state['deadline_protocol'] == 2:
                if state.get('guard_pid') != pid:
                    raise ValueError('Replacement guard identity cannot be verified')
                return state
            candidate = root / 'deadline-guard-v2.py'
            publish(candidate, request['source'])
            # Compile before suspending the owner. Its original file remains untouched.
            compile(request['source'], str(candidate), 'exec')
            signal.pidfd_send_signal(descriptor, signal.SIGSTOP)
            stopped = True
            stop_until = time.monotonic() + 2
            while time.monotonic() < stop_until:
                if select.select([descriptor], [], [], 0)[0]:
                    raise ValueError('Old guard exited before handover')
                status = (Path('/proc') / str(pid) / 'stat').read_text().rpartition(')')[2].split()[0]
                if status in ('T', 't'):
                    break
                time.sleep(.05)
            else:
                raise ValueError('Old guard did not pause')
            state = guard_state(root, request)
            if request_path.is_symlink() or archive_path.is_symlink():
                raise ValueError('Linked deadline request')
            if request_path.exists():
                # A rejected protocol-1 request might become valid under protocol 2.
                os.replace(request_path, archive_path)
                archived_request = True
            env.update(PBGUI_WORKDIR=str(root), PBGUI_DEADLINE=str(state['deadline']),
                       PBGUI_MAX_DEADLINE=str(state['max_deadline']),
                       PBGUI_HARD_DEADLINE=str(min(request['hard_deadline'], state.get('hard_deadline', request['hard_deadline']))))
            child = subprocess.Popen([sys.executable, str(candidate), 'guard'], env=env,
                                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL, start_new_session=True)
            until = time.monotonic() + READY_TIMEOUT
            while time.monotonic() < until:
                if child.poll() is not None:
                    raise ValueError('Replacement guard failed to start')
                current = guard_state(root, request)
                if current.get('deadline_protocol') == 2 and current.get('guard_pid') == child.pid:
                    signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                    committed = True
                    if archived_request:
                        archive_path.unlink()
                    return current
                time.sleep(.05)
            raise ValueError('Replacement guard did not confirm readiness')
        finally:
            try:
                if stopped and not committed:
                    try:
                        if child is not None and child.poll() is None:
                            child.kill()
                            child.wait(timeout=5)
                    finally:
                        try:
                            # Restore the exact record, including prior acknowledgements.
                            publish(root / 'guard.json', state)
                        finally:
                            try:
                                if archived_request:
                                    os.replace(archive_path, request_path)
                            finally:
                                if not select.select([descriptor], [], [], 0)[0]:
                                    signal.pidfd_send_signal(descriptor, signal.SIGCONT)
            finally:
                os.close(descriptor)
    finally:
        os.close(lock_fd)


def main():
    """Accept public handover metadata and source; never emit container credentials."""
    def interrupted(signum, frame):
        """Route ordinary SSH disconnection/termination through rollback cleanup."""
        raise InterruptedError('Guard migration interrupted')

    for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        signal.signal(signum, interrupted)
    try:
        raw = sys.stdin.buffer.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError('Oversized migration request')
        result = migrate(ROOT, json.loads(raw))
        sys.stdout.write(json.dumps({'ok': True, 'guard': result}))
    except Exception as exc:
        # Error text may contain OS paths or supplied data. Return only its class.
        sys.stdout.write(json.dumps({'ok': False, 'error': type(exc).__name__}))
        sys.exit(1)


if __name__ == '__main__':
    main()
