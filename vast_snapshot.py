"""Bounded, reentrant Vast transactions compatible with already loaded file_lock."""
from contextlib import contextmanager, ExitStack
from contextvars import ContextVar
from functools import wraps
from pathlib import Path
import fcntl
import errno
import os
import time

from file_lock import _thread_lock, _HELD_LOCKS
from vast_provider import VastError

SERVICE = 'VastSnapshot'
WORK_CHECK = ContextVar('vast_work_check', default=None)


def check_work():
    """Return remaining manual work time, or leave ordinary callers unchanged."""
    check = WORK_CHECK.get()
    return check() if check is not None else None


def sleep_work(seconds):
    """Keep provider pacing cancellable without changing its required interval."""
    if WORK_CHECK.get() is None:
        time.sleep(seconds)
        return
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        allowance = check_work()
        time.sleep(min(.1, max(0, until - time.monotonic()), allowance))


@contextmanager
def work_boundary(check):
    """Own a request-local deadline without leaking it into supervisor work."""
    token = WORK_CHECK.set(check)
    try:
        yield
    finally:
        WORK_CHECK.reset(token)


@contextmanager
def bounded_lock(target, *, timeout=None, check=lambda: None, check_on_acquire=True, blocking=True):
    """Use file_lock's exact lock identity; never recreate a missing parent."""
    supplied = Path(target).expanduser().absolute()
    if '..' in supplied.parts or any(ord(char) < 32 for char in str(supplied)):
        raise VastError('Unsafe collection lock path', 409)
    # Match advisory_file_lock even when data/vast lives on another volume.
    # Keep the supplied parent too: retargeting an ancestor during admission
    # must not silently lock the old directory while work uses the new one.
    target = supplied.resolve(strict=False)
    parent = target.parent
    if target.with_name(target.name + '.lock').is_symlink():
        raise VastError('Unsafe collection lock path', 409)
    try:
        before = parent.stat()
    except FileNotFoundError:
        raise VastError('Cloud job is no longer available', 404) from None
    key = str(target)
    lock = _thread_lock(key)
    deadline = None if timeout is None else time.monotonic() + timeout

    def waiting():
        """Bound both thread and process waits by the same allowance."""
        check()
        check_work()
        if deadline is not None and time.monotonic() >= deadline:
            raise VastError('Collection is busy; current work continues automatically', 409)

    while not lock.acquire(blocking=False):
        if not blocking:
            raise VastError('Collection is busy; current work continues automatically', 409)
        waiting()
        time.sleep(.025)
    stream = None
    entered = False
    try:
        if check_on_acquire:
            waiting()
        held = getattr(_HELD_LOCKS, 'entries', None)
        if held is None:
            held = {}
            _HELD_LOCKS.entries = held
        if key in held:
            held[key]['depth'] += 1
            entered = True
        else:
            # Opening relative to the verified parent prevents rename races from
            # creating a replacement job directory or following a lock symlink.
            fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                if (os.fstat(fd).st_dev, os.fstat(fd).st_ino) != (before.st_dev, before.st_ino):
                    raise VastError('Cloud job changed during collection', 409)
                lock_fd = os.open(target.name + '.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=fd)
            finally:
                os.close(fd)
            stream = os.fdopen(lock_fd, 'a+b')
            os.fchmod(stream.fileno(), 0o600)
            while True:
                try:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if not blocking:
                        raise VastError('Collection is busy; current work continues automatically', 409)
                    waiting()
                    time.sleep(.025)
            held[key] = {'depth': 1, 'file': stream}
            entered = True
        after = parent.stat()
        if (supplied.resolve(strict=False) != target
                or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)):
            raise VastError('Cloud job changed during collection', 409)
        yield
    except FileNotFoundError:
        raise VastError('Cloud job is no longer available', 404) from None
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise VastError('Unsafe collection lock path', 409) from None
        raise
    finally:
        if entered:
            held[key]['depth'] -= 1
            if held[key]['depth'] == 0:
                entry = held.pop(key)
                fcntl.flock(entry['file'].fileno(), fcntl.LOCK_UN)
                entry['file'].close()
        elif stream is not None:
            stream.close()
        lock.release()


@contextmanager
def snapshot(store, identifier, *, check=lambda: None):
    """Protect source files and metadata against replacement and deletion."""
    directory = store.directory(identifier)
    with bounded_lock(directory / '.snapshot-lock', check=check):
        if store.directory(identifier) != directory:
            raise VastError('Cloud job changed during collection', 409)
        yield


def snapshot_method(function):
    """Make direct transport callers participate in the same transaction."""
    @wraps(function)
    def locked(self, *args, **kwargs):
        """Keep the existing transport signature and fake-connection tests usable."""
        with bounded_lock(self.directory / '.snapshot-lock'):
            return function(self, *args, **kwargs)
    return locked


def snapshot_import(function):
    """Protect every native importer, including direct keyword callers."""
    @wraps(function)
    def locked(store, identifier, *args, **kwargs):
        """Retain source ownership through the complete native import."""
        with snapshot(store, identifier, check=kwargs.get('check', lambda: None)):
            return function(store, identifier, *args, **kwargs)
    return locked


@contextmanager
def deletion_snapshots(store, identifiers):
    """Take every deletion lock before tombstones, history deletion or renames."""
    with ExitStack() as stack:
        for identifier in sorted(identifiers):
            directory = store.directory(identifier)
            stack.enter_context(bounded_lock(directory / '.finalize-lock', blocking=False))
            stack.enter_context(bounded_lock(directory / '.snapshot-lock', blocking=False))
        yield
