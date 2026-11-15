"""Cross-generation exchange-account guard for Passivbot live starts.

One exchange account must be traded by at most one Passivbot generation. PBRun
holds ``account_start_lock`` through process creation for every PB7, PB8 and
PB9 start, and the live APIs call ``check_account_conflict`` (under
``account_lock``) before enabling an instance. A conflict is always rejected;
this module never stops, signals or rewrites another bot.

Only non-secret instance metadata is inspected: ``config.json`` account names
and PBGui ``enabled_on`` targets. Credential files are never opened, and this
module neither logs nor imports PBRun.
"""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import stat
import threading
import time
from typing import Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - PBGui production targets Linux
    fcntl = None

try:
    import psutil
except ImportError:  # pragma: no cover - psutil is a PBGui runtime dependency
    psutil = None


SERVICE = "PassivbotAccountGuard"
MAX_CONFIG_BYTES = 8 * 1024 * 1024
DISABLED_TARGET = "disabled"
DEFAULT_LOCK_TIMEOUT = 10.0
# generation -> (run directory below data/, label, config file named on the bot command line)
_RUNTIMES = {
    "pb7": ("run_v7", "PB7", "config_run.json"),
    "pb8": ("run_v8", "PB8", "config.json"),
    "pb9": ("run_v9", "PB9", "config.json"),
}
_MALFORMED = object()
_THREAD_LOCKS = tuple(threading.RLock() for _ in range(64))
_HELD_LOCKS = threading.local()


class AccountConflictError(RuntimeError):
    """Reject a start or enable request because account ownership is not exclusive.

    ``code`` is one of ``enabled_elsewhere``, ``process_running``,
    ``unverifiable``, ``lock_busy`` or ``invalid_request``. ``runtime`` and
    ``instance`` name the other owner when one was identified.
    """

    def __init__(self, code: str, message: str, *, runtime: str = "", instance: str = ""):
        """Keep stable, non-secret fields for API error mapping."""
        self.code = code
        self.runtime = runtime
        self.instance = instance
        super().__init__(message)


class AccountLockBusyError(AccountConflictError):
    """The shared account lock could not be acquired within the bounded wait."""


def _account(user) -> str:
    """Return the exact account key shared by PB7, PB8 and PB9 api-keys files."""
    if not isinstance(user, str) or not user.strip() or len(user) > 256 or any(
            ord(char) < 32 or ord(char) == 127 for char in user):
        raise AccountConflictError("invalid_request", "Account name is missing or invalid")
    return user.strip()


def _generation(runtime) -> str:
    """Normalize one supported runtime generation."""
    value = str(runtime or "").strip().lower()
    if value not in _RUNTIMES:
        raise AccountConflictError("invalid_request", "Runtime generation must be pb7, pb8 or pb9")
    return value


def _real(path) -> str:
    """Canonicalize a path for identity comparison without requiring existence."""
    try:
        return str(Path(path).expanduser().resolve(strict=False))
    except (OSError, RuntimeError, TypeError, ValueError):
        return ""


def _read_object(path: Path):
    """Return a JSON value, None when absent, or ``_MALFORMED`` when unsafe or unreadable."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError:
        return _MALFORMED
    try:
        with os.fdopen(fd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                return _MALFORMED
            source = stream.read(MAX_CONFIG_BYTES + 1)
        if len(source) > MAX_CONFIG_BYTES:
            return _MALFORMED
        return json.loads(source)
    except (OSError, ValueError, RecursionError):
        return _MALFORMED


def _enabled(metadata) -> bool | None:
    """Return whether PBGui metadata assigns a host; None when it cannot be verified."""
    if metadata is None:
        return False
    if not isinstance(metadata, dict):
        return None
    target = metadata.get("enabled_on")
    if target is None:
        return False
    if not isinstance(target, str):
        return None
    target = target.strip()
    return bool(target) and target != DISABLED_TARGET


def _instance_state(generation: str, directory: Path) -> tuple[str, bool | None] | None:
    """Return ``(account, enabled)`` for one instance, or None without a config.

    An unreadable account falls back to the directory name, PB7's own naming
    rule. ``enabled`` is None when metadata exists but cannot be interpreted.
    """
    config = _read_object(directory / "config.json")
    if config is None:
        return None
    if not isinstance(config, dict):
        return directory.name, None
    live = config.get("live") if isinstance(config.get("live"), dict) else {}
    if generation == "pb9":
        account = live.get("account") if isinstance(live.get("account"), dict) else {}
        user = account.get("user")
        metadata = _read_object(directory / "pbgui.json")
        enabled = None if metadata is _MALFORMED else _enabled(metadata)
    else:
        user = live.get("user")
        enabled = _enabled(config.get("pbgui"))
    if not isinstance(user, str) or not user.strip():
        return directory.name, enabled
    return user.strip(), enabled


def _running_reference(references: dict[str, tuple[str, str]]) -> tuple[str, str] | None:
    """Return the owner of a config path named on any live process command line."""
    if psutil is None or not references:
        return None
    for process in psutil.process_iter():
        try:
            cmdline = process.cmdline()
        except (psutil.Error, OSError):
            continue
        for argument in cmdline:
            owner = references.get(str(argument))
            if owner is not None:
                return owner
    return None


def check_account_conflict(pbgdir, user, runtime, instance_path) -> None:
    """Raise ``AccountConflictError`` when another generation owns ``user``.

    Instances of the other two generations below ``pbgdir/data`` conflict when
    they name the same account and are enabled on any host, when their metadata
    cannot prove they are disabled, or when a disabled one still has a live
    process. Disabled, stale configs do not block. Instances of ``runtime``
    itself and ``instance_path`` are never inspected. Read-only.
    """
    account = _account(user)
    generation = _generation(runtime)
    own = _real(instance_path) if instance_path else ""
    dormant: dict[str, tuple[str, str]] = {}
    for other, (folder, label, process_config) in _RUNTIMES.items():
        if other == generation:
            continue
        run_root = Path(pbgdir) / "data" / folder
        if run_root.is_symlink() or run_root.parent.is_symlink():
            raise AccountConflictError("unverifiable", "Account ownership storage is unsafe", runtime=other)
        try:
            entries = sorted(run_root.iterdir(), key=lambda item: item.name)
        except (FileNotFoundError, NotADirectoryError):
            continue
        except OSError as exc:
            raise AccountConflictError(
                "unverifiable", f"Cannot inspect {label} instances for account ownership", runtime=other) from exc
        for directory in entries:
            try:
                if directory.name.startswith(".") or not directory.is_dir():
                    continue
            except OSError:
                continue
            if own and _real(directory) == own:
                continue
            state = _instance_state(other, directory)
            if state is None or state[0] != account:
                continue
            if state[1] is False:
                for path in (directory / process_config, Path(_real(directory)) / process_config):
                    dormant[str(path)] = (other, directory.name)
                continue
            if state[1] is None:
                raise AccountConflictError(
                    "unverifiable",
                    f"Account '{account}' may be used by {label} instance '{directory.name}', "
                    "whose configuration cannot be verified as disabled",
                    runtime=other, instance=directory.name)
            raise AccountConflictError(
                "enabled_elsewhere",
                f"Account '{account}' is already assigned to enabled {label} instance '{directory.name}'",
                runtime=other, instance=directory.name)
    running = _running_reference(dormant)
    if running is not None:
        raise AccountConflictError(
            "process_running",
            f"Account '{account}' is still traded by a running {_RUNTIMES[running[0]][1]} "
            f"process of disabled instance '{running[1]}'",
            runtime=running[0], instance=running[1])


def _thread_lock(key: str) -> threading.RLock:
    """Use a bounded lock stripe while the file lock retains exact account ownership."""
    return _THREAD_LOCKS[int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % len(_THREAD_LOCKS)]


@contextmanager
def account_lock(pbgdir, user, *, timeout: float = DEFAULT_LOCK_TIMEOUT) -> Iterator[None]:
    """Hold the exclusive cross-process lock of one account, reentrantly per thread.

    The same lock file serves every generation, so enabling or starting an
    account is serialized across PBRun and the live APIs. A busy lock raises
    ``AccountLockBusyError`` after ``timeout`` seconds instead of waiting
    indefinitely. The descriptor is not inherited by started bots.
    """
    account = _account(user)
    lock_dir = Path(pbgdir) / "data" / "locks" / "accounts"
    path = lock_dir / (hashlib.sha256(account.encode("utf-8")).hexdigest()[:40] + ".lock")
    key = str(path)
    held = getattr(_HELD_LOCKS, "keys", None)
    if held is None:
        held = _HELD_LOCKS.keys = set()
    if key in held:
        yield
        return
    deadline = time.monotonic() + max(0.0, float(timeout))
    thread_lock = _thread_lock(key)
    if not thread_lock.acquire(timeout=max(0.0, float(timeout))):
        raise AccountLockBusyError("lock_busy", f"Account '{account}' is being started or changed elsewhere")
    try:
        fd = -1
        try:
            for directory in (Path(pbgdir), Path(pbgdir) / "data", lock_dir.parent, lock_dir):
                if directory.is_symlink():
                    raise OSError("Account lock directory must not use symlink parents")
            lock_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
            while fcntl is not None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise AccountLockBusyError(
                            "lock_busy", f"Account '{account}' is being started or changed elsewhere") from None
                    time.sleep(0.05)
        except OSError as exc:
            if fd >= 0:
                os.close(fd)
            raise AccountLockBusyError("lock_busy", "Account lock is unavailable") from exc
        except AccountLockBusyError:
            os.close(fd)
            raise
        held.add(key)
        try:
            yield
        finally:
            held.discard(key)
            try:
                if fcntl is not None:
                    fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
    finally:
        thread_lock.release()


@contextmanager
def account_start_lock(pbgdir, user, runtime, instance_path, *,
                       timeout: float = DEFAULT_LOCK_TIMEOUT) -> Iterator[None]:
    """Hold the account lock and reject a conflicting owner before yielding.

    Callers create the bot process inside the block, so a concurrent start or
    enable request for the same account observes a consistent state.
    """
    with account_lock(pbgdir, user, timeout=timeout):
        check_account_conflict(pbgdir, user, runtime, instance_path)
        yield
