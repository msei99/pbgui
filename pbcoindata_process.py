"""Lightweight exact PBCoinData process ownership and bounded local actions."""

from __future__ import annotations

from contextlib import contextmanager, ExitStack
from dataclasses import dataclass, field
from enum import Enum
import json
import math
import os
from pathlib import Path
import re
import shlex
import stat
import signal
import subprocess
import sys
import threading
import time
import uuid

import psutil

from logging_helpers import human_log as _log
from process_identity import ProcessIdentity, identities_match, identity_path, signal_exact_process, write_process_identity
from secure_files import atomic_write_private_text, ensure_private_directory
from service_lifecycle_lock import acquire_service_lifecycle_lock
from pbcoindata_guard import FileLease
import pbcoindata_guard as guard_protocol

SERVICE = "PBCoinData"
UNIT = "pbgui-pbcoindata.service"
_CHILDREN = {}
_CHILDREN_LOCK = threading.RLock()
_SYSTEMD_ATTEMPTS = {}
_SYSTEMD_ATTEMPTS_LOCK = threading.RLock()


class ProcessStateError(RuntimeError):
    """Refuse unsafe or unconfirmed process transitions."""


class RetryDeferred(ProcessStateError):
    """An automatic start is not due; no spawn failure has occurred."""


class Awaiting(Enum):
    """Observation only: none of these phases grants ownership or signal rights."""
    UNIT = "unit_start"
    GUARD = "guard_publication"
    PUBLICATION = "process_publication"
    SAMPLE = "coherent_sample"


class SampleProgress(ProcessStateError):
    """A positively bound invocation changed a sample while it was being read."""

    def __init__(self, message, holders=(), *, exited_pid=None):
        """Retain concrete grants so a foreign owner cannot become retryable progress."""
        super().__init__(message)
        self.holders = holders
        self.exited_pid = exited_pid


@dataclass(frozen=True)
class GuardObservation:
    """A read-only kernel lock sample, never a lock-acquisition shortcut."""
    root: Path
    inode: tuple | None
    payload: bytes
    holder: ProcessIdentity | None

    def matches(self, registration, identity):
        """Require a granted lock, exact daemon payload and published inode."""
        try:
            payload = json.loads(self.payload)
            return (isinstance(payload, dict) and identities_match(self.holder, identity)
                    and self.inode == (registration['guard_device'], registration['guard_inode'])
                    and payload.get('owner') == 'daemon' and payload.get('protocol') == guard_protocol.PROTOCOL
                    and identities_match(identity, ProcessIdentity(payload['pid'], payload['create_time'])))
        except (ValueError, TypeError, KeyError):
            return False


@dataclass(frozen=True)
class PublicationObservation:
    """One byte-bound publication/journal snapshot with independently observed lock evidence."""
    root: Path
    bundle: dict
    journal: bytes | None
    guard: GuardObservation


@dataclass
class SystemdAttempt:
    """A transient, authorization-owned observation window for one manual command."""
    authorization: object
    deadline: float
    before: dict
    starting: bool
    invocation: str | None = None
    identity: ProcessIdentity | None = None
    assignment: tuple | None = None
    guard_inode: tuple | None = None
    active: bool = True
    lock: object = field(default_factory=threading.RLock)

    def valid(self, root):
        """Read-only consumers may share this context but acquire no action rights."""
        auth = self.authorization
        info = Path(root).resolve().stat()
        return (self.active and auth.active and auth.root == Path(root).resolve()
                and auth.uid == os.getuid() and auth.checkout == (info.st_dev, info.st_ino)
                and time.monotonic() < self.deadline)

    def bind(self, root, unit, identity):
        """Reject replacement Invocations and lifetimes rather than adopting a retry."""
        with self.lock:
            if not self.valid(root):
                raise ProcessStateError('Systemd observation attempt expired or changed')
            if self.starting and self.identity is not None and identity is None:
                raise ProcessStateError('Bound PBCoinData MainPID disappeared during initialization')
            invocation = unit['InvocationID']
            if not re.fullmatch('[0-9a-f]{32}', invocation) or invocation == '0' * 32:
                raise ProcessStateError('Systemd InvocationID is unconfirmed')
            previous = self.before['_unit']['InvocationID']
            if self.invocation is None:
                if self.starting and not self.before['running'] and invocation == previous:
                    raise ProcessStateError('Systemd did not establish a new start InvocationID')
                self.invocation = invocation
            elif invocation != self.invocation:
                raise ProcessStateError('Systemd InvocationID changed during observation')
            if identity is not None:
                assignment = tuple(unit[k] for k in ('Id','ExecStart','WorkingDirectory','ControlGroup'))
                if self.identity is not None and (not identities_match(self.identity, identity) or self.assignment != assignment):
                    raise ProcessStateError('PBCoinData lifetime or unit changed during observation')
                self.identity, self.assignment = identity, assignment


def _bounded_fd_read(fd, deadline, limit):
    """Read through EOF within a fixed byte limit and the caller's absolute deadline."""
    from pbcoindata_manager import _check
    chunks, count = [], 0
    while True:
        _check(deadline)
        chunk = os.read(fd, min(16384, limit + 1 - count))
        _check(deadline)
        if not chunk:
            return b''.join(chunks)
        count += len(chunk)
        if count > limit:
            raise ProcessStateError('Guard observation exceeds its read limit')
        chunks.append(chunk)


def _lock_holder(text, inode):
    """Parse the complete kernel listing; waiting or unsupported locks cannot prove absence."""
    if text and not text.endswith('\n') or len(text.splitlines()) > 32768:
        raise ProcessStateError('Kernel lock listing is truncated or oversized')
    grants, waiting = [], False
    for line in text.splitlines():
        fields = line.split()
        if not fields or not re.fullmatch('[0-9]+:', fields[0]):
            raise ProcessStateError('Kernel lock listing is malformed')
        fields = fields[1:]
        blocked = fields[:1] == ['->']
        if blocked: fields = fields[1:]
        if (len(fields) != 7 or fields[0] not in {'FLOCK','POSIX','OFDLCK','ACCESS','LEASE','DELEG','UNKNOWN'}
                or fields[1] not in {'ADVISORY','ACTIVE','BREAKING','BREAKER','*NOINODE*','UNKNOWN'}
                or fields[2] not in {'WRITE','READ','UNLCK'} or not re.fullmatch('-?[0-9]+', fields[3])
                or not re.fullmatch('[0-9]+', fields[5]) or not re.fullmatch('EOF|[0-9]+', fields[6])):
            raise ProcessStateError('Kernel lock record is incomplete')
        device = re.fullmatch('([0-9a-f]+):([0-9a-f]+):([0-9]+)', fields[4])
        if device is None:
            if fields[4] == '<none>:0': continue
            raise ProcessStateError('Kernel lock device is malformed')
        if (len(device[1]) > 8 or len(device[2]) > 8 or len(device[3]) > 20
                or not 0 < int(device[3]) < 2**64 or len(fields[3]) > 11
                or not -1 <= int(fields[3]) < 2**31):
            raise ProcessStateError('Kernel lock identity exceeds its value range')
        current = (os.makedev(int(device[1],16), int(device[2],16)), int(device[3]))
        if current != inode: continue
        if blocked:
            waiting = True
        elif fields[:3] != ['FLOCK','ADVISORY','WRITE'] or fields[5:] != ['0','EOF'] or int(fields[3]) <= 1:
            raise ProcessStateError('Singleton has an unsupported or unidentified granted lock')
        else:
            grants.append(int(fields[3]))
    if len(grants) > 1 or waiting and not grants:
        raise ProcessStateError('Singleton lock ownership is ambiguous')
    return grants[0] if grants else None


def read_systemd_guard(root, deadline):
    """Observe an authenticated procfs lock listing without creating, writing or locking anything."""
    import pbcoindata_manager as manager
    root = Path(root).resolve()
    path = root / 'data/locks/pbcoindata-singleton.lock'
    with ExitStack() as stack:
        view = manager._manual_start_view(root, deadline)
        # Keep mount references pinned across the lock listing and fresh view checks.
        for pid in ('self', 1):
            if manager._mountinfo_fd(pid, stack, root, deadline) != view[1]:
                raise ProcessStateError('Procfs view changed before lock observation')
        proc, _ = manager._mount_view(view[1], root)
        lock_fd = None
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
        stack.callback(os.close, directory)
        try:
            for component in ('data','locks'):
                directory = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=directory)
                stack.callback(os.close, directory)
                if os.fstat(directory).st_uid != os.getuid():
                    raise ProcessStateError('Singleton directory ownership is unconfirmed')
            lock_fd = os.open(path.name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=directory)
            stack.callback(os.close, lock_fd)
        except FileNotFoundError:
            pass
        info = os.fstat(lock_fd) if lock_fd is not None else None
        if info is not None and (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1):
            raise ProcessStateError('Singleton file identity is unconfirmed')
        inode = (info.st_dev, info.st_ino) if info is not None else None
        before = _bounded_fd_read(lock_fd, deadline, 4096) if lock_fd is not None else b''
        holders = []
        births = []
        for _ in range(2):
            fd = os.open('/proc/locks', os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
            try:
                metadata = os.fstat(fd)
                manager._procfs_fd(fd)
                mount_ids = re.findall(r'^mnt_id:\s*([0-9]+)\s*$', manager._read(f'/proc/self/fdinfo/{fd}',deadline), re.M)
                if (not stat.S_ISREG(metadata.st_mode) or metadata.st_dev != os.makedev(*proc.device)
                        or mount_ids != [str(proc.ident)]):
                    raise ProcessStateError('Kernel lock listing is not attached to the verified procfs')
                text = _bounded_fd_read(fd, deadline, 2 * 1024 * 1024).decode('ascii')
            finally:
                os.close(fd)
            owner = _lock_holder(text, inode)
            holders.append(owner)
            try:
                births.append(ProcessIdentity(owner, psutil.Process(owner).create_time()) if owner is not None else None)
            except psutil.NoSuchProcess as exc:
                raise SampleProgress('Granted lock holder exited during observation', exited_pid=owner) from exc
        if holders[0] != holders[1] or births[0] != births[1]:
            raise SampleProgress('Singleton grant changed during observation', tuple(b for b in births if b is not None))
        if lock_fd is not None:
            os.lseek(lock_fd, 0, os.SEEK_SET)
            after = _bounded_fd_read(lock_fd, deadline, 4096)
            current = path.lstat()
            if (current.st_dev, current.st_ino) != inode:
                raise ProcessStateError('Singleton inode changed during observation')
            if before != after:
                raise SampleProgress('Singleton payload changed during observation', tuple(b for b in births if b is not None))
        elif path.exists():
            raise SampleProgress('Singleton appeared during observation')
        if manager._manual_start_view(root, deadline) != view:
            raise ProcessStateError('Procfs namespace or mount view changed during lock observation')
        return GuardObservation(root, inode, before, births[0])


def _publication_bytes(root):
    """Read only concrete publication files, retaining exact bytes for coherence checks."""
    path = pid_path(root)
    return {p: guard_protocol._read_publication_file(p, root) for p in (path, identity_path(path), guard_protocol.registration_path(root))}


def _collect_observation(root, guard):
    """Bind parsed publications and journal to unchanged original bytes."""
    root = Path(root).resolve()
    before = _publication_bytes(root)
    journal = guard_protocol._read_publication_file(guard_protocol.journal_path(root), root)
    document = guard_protocol.read_journal(root)
    if document['pending_launch'] is not None:
        raise ProcessStateError('Unresolved PBCoinData launch prevents systemd observation')
    try:
        bundle = guard_protocol.read_publications(root)
    except (ValueError, TypeError, KeyError):
        if before != _publication_bytes(root):
            raise SampleProgress('Publication changed during decoding')
        raise
    if (bundle['files'] != before or _publication_bytes(root) != before
            or guard_protocol._read_publication_file(guard_protocol.journal_path(root), root) != journal):
        raise SampleProgress('PBCoinData publications changed during observation')
    return PublicationObservation(root, bundle, journal, guard)


@contextmanager
def actions(root, action):
    """Acquire only reentrant action locks, never a controller election lease."""
    leases = []
    deadline = time.monotonic() + 5
    try:
        for name in ("all-services", "pbcoindata"):
            leases.append(acquire_service_lifecycle_lock(Path(root), name, action, timeout=max(0, deadline - time.monotonic())))
        yield
    finally:
        for lease in reversed(leases):
            lease.release()


def pid_path(root):
    """Return the compatibility PID path below this checkout."""
    return Path(root).resolve() / "data" / "pid" / "pbcoindata.pid"


def inspect_pid(root, pid, expected=None, *, script="PBCoinData.py"):
    """Distinguish confirmed exit, foreign ownership and inaccessible identity."""
    try:
        if type(pid) is not int or pid <= 1:
            raise ValueError("Invalid PID")
        process = psutil.Process(pid)
        current = ProcessIdentity(pid, float(process.create_time()))
        if expected is not None and not identities_match(current, expected):
            return {"status": "unknown", "error": "Process identity changed", "pid": pid}
        if process.status() in {psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD}:
            fresh = ProcessIdentity(pid, float(psutil.Process(pid).create_time()))
            if not identities_match(current, fresh):
                return {"status": "unknown", "error": "Process identity changed during exit confirmation", "pid": pid}
            return {"status": "stopped", "pid": None, "identity": current}
        uids = process.uids()
        argv = process.cmdline()
        root = Path(root).resolve()
        cwd = Path(process.cwd()).resolve()
        arguments = argv[1:]
        if arguments[:1] == ["-u"]:
            arguments = arguments[1:]
        owned = (uids.real == os.getuid() and uids.effective == os.getuid() and cwd == root
                 and argv and re.fullmatch(r"python(?:\d+(?:\.\d+)*)?", Path(argv[0]).name)
                 and len(arguments) == 1 and not any(ord(c) < 32 for c in arguments[0])
                 and (cwd / arguments[0]).resolve() == (root / script).resolve())
        if not owned:
            return {"status": "foreign", "pid": pid, "error": "PID is not this checkout's exact Python service"}
        if not identities_match(current, ProcessIdentity(pid, float(process.create_time()))):
            raise ProcessStateError("Process identity changed during inspection")
        return {"status": "running", "pid": pid, "identity": current}
    except (psutil.NoSuchProcess, ProcessLookupError):
        return {"status": "stopped", "pid": None}
    except Exception as exc:
        _log(SERVICE, f"Exact process inspection unavailable: {type(exc).__name__}", level="WARNING")
        return {"status": "unknown", "pid": pid, "error": f"Process inspection unavailable: {type(exc).__name__}"}


def _stopped(root, singleton_owned, observation=None):
    """An unpublished or exiting singleton is not proof of process absence."""
    if not singleton_owned:
        if observation is not None:
            if not isinstance(observation, PublicationObservation) or observation.root != Path(root).resolve():
                raise ProcessStateError('A bound read-only publication observation is required')
            if observation.guard.holder is not None:
                return {"status":"unknown", "pid":observation.guard.holder.pid,
                        "error":"PBCoinData singleton transition is unconfirmed"}
            return {"status":"stopped", "pid":None}
        launch = _read_launch(root)
        if launch is not None and _launch_active(launch):
            return {"status": "unknown", "pid": launch["pid"], "error": "Previous PBCoinData start remains unconfirmed"}
        try:
            probe = FileLease(root, "singleton", "state-probe")
        except BlockingIOError:
            return {"status": "unknown", "pid": None, "error": "PBCoinData singleton transition is unconfirmed"}
        else:
            probe.release()
    return {"status": "stopped", "pid": None}


def _publication_state(root, record):
    """Use the same historical-publication policy with psutil process observations."""
    def created(pid):
        """Translate only confirmed PID exit, preserving permission failures as UNKNOWN."""
        try:
            return psutil.Process(pid).create_time()
        except psutil.NoSuchProcess as exc:
            raise ProcessLookupError(pid) from exc
    def foreign(pid):
        """Gather complete data only for a concrete referenced PID without old identity."""
        process = psutil.Process(pid)
        argv = process.cmdline()
        cwd = Path(process.cwd()).resolve()
        args = argv[1:]
        if args[:1] == ["-u"]:
            args = args[1:]
        if args and (cwd / args[0]).resolve() == (Path(root) / "PBCoinData.py").resolve():
            return False
        uids = process.uids()
        return guard_protocol.foreign_publication(root, pid, details={"argv": argv, "cwd": cwd,
            "exe": process.exe(), "uids": (uids.real, uids.effective)})
    try:
        return guard_protocol.publication_state(root, record, current_time=created, foreign=foreign)
    except psutil.Error:
        return "unknown"


def snapshot(root, *, singleton_owned=False, observation=None):
    """Observe old publication lifetimes separately from the current PID holder."""
    try:
        if observation is not None and (not isinstance(observation, PublicationObservation) or observation.root != Path(root).resolve()):
            raise ProcessStateError('Snapshot observation is not bound to this checkout')
        bundle = observation.bundle if observation is not None else guard_protocol.read_publications(root)
        active = []
        for record in bundle["records"]:
            status = _publication_state(root, record)
            if (singleton_owned and record["kind"] == "registration" and record.get("phase") == "starting"
                    and record["pid"] == os.getpid() and status == "active"
                    and any(g.root == Path(root).resolve() and g.fd is not None for g in guard_protocol._LIFETIME_GUARDS)):
                continue
            if status == "unknown":
                return {"status": "unknown", "pid": record["pid"], "error": "PBCoinData publication lifetime is unconfirmed"}
            if status == "active":
                active.append(record)
        if not active:
            return _stopped(root, singleton_owned, observation)
        numeric = next((r for r in active if r["kind"] == "pid"), None)
        if numeric is None or any(r["pid"] != numeric["pid"] for r in active):
            return {"status": "unknown", "pid": None, "error": "Active PBCoinData publications are incomplete or conflicting"}
        if any(r["kind"] == "registration" and r["phase"] != "ready" for r in active):
            return {"status": "unknown", "pid": numeric["pid"], "error": "PBCoinData readiness is unconfirmed"}
        expected = ProcessIdentity(numeric["pid"], numeric["create_time"]) if "create_time" in numeric else None
        state = inspect_pid(root, numeric["pid"], expected)
        return _stopped(root, singleton_owned, observation) if state["status"] == "stopped" else state
    except Exception as exc:
        _log(SERVICE, f"Process identity file unavailable: {type(exc).__name__}", level="WARNING")
        return {"status": "unknown", "pid": None, "error": f"Identity file unavailable: {type(exc).__name__}"}


def register_current(root):
    """Publish this singleton's exact identity; caller owns the lifetime singleton."""
    current = ProcessIdentity(os.getpid(), psutil.Process().create_time())
    before = guard_protocol.read_publications(root)["files"]
    previous = snapshot(root, singleton_owned=True)
    if previous["status"] != "stopped":
        raise ProcessStateError("Existing process identity is not confirmed stopped")
    path = pid_path(root)
    guard_protocol.publications_unchanged(root, before)
    ensure_private_directory(path.parent)
    write_process_identity(path, current)
    atomic_write_private_text(path, str(current.pid))
    guards = [g for g in guard_protocol._LIFETIME_GUARDS if g.root == Path(root).resolve() and g.fd is not None]
    if guards:
        guard_protocol.publish(guards[-1], "ready")
    return current


def clear_owned_files(root, expected):
    """Serialize post-exit cleanup with direct starts, retaining a live daemon's own guard."""
    cleanup = None
    try:
        own_guard = any(g.root == Path(root).resolve() and g.fd is not None
                        and identities_match(expected, ProcessIdentity(g.identity["pid"], g.identity["create_time"]))
                        and expected.pid == os.getpid() for g in guard_protocol._LIFETIME_GUARDS)
        if not own_guard:
            try:
                cleanup = FileLease(root, "singleton", "cleanup-probe")
            except BlockingIOError:
                return  # A newer/direct daemon owns its publications now.
        path = pid_path(root)
        before = guard_protocol.read_publications(root)["files"]
        value = json.loads(identity_path(path).read_text(encoding="utf-8"))
        recorded = ProcessIdentity(int(value["pid"]), float(value["create_time"]))
        if identities_match(recorded, expected):
            try:
                value = path.read_text().strip()
            except FileNotFoundError:
                value = str(expected.pid)
            if value == str(expected.pid):
                guard_protocol.publications_unchanged(root, before)
                path.unlink(missing_ok=True)
                before = {**before, path: None}
                guard_protocol.publications_unchanged(root, before)
                identity_path(path).unlink()
    except FileNotFoundError:
        pass
    except Exception as exc:
        _log(SERVICE, f"Identity cleanup unconfirmed: {type(exc).__name__}", level="WARNING")
    finally:
        if cleanup is not None:
            cleanup.release()


def reap_children():
    """Collect ended children without waiting threads or pipes."""
    with _CHILDREN_LOCK:
        for pid, child in list(_CHILDREN.items()):
            if child.poll() is not None:
                child.wait(timeout=0)
                _CHILDREN.pop(pid, None)


def _wait_exit(root, identity, timeout):
    """Observe the signalled lifetime's kernel exit, without authorizing another signal."""
    deadline = time.monotonic() + timeout
    while True:
        inspection_error = None
        try:
            process = psutil.Process(identity.pid)
            current = ProcessIdentity(identity.pid, float(process.create_time()))
            if not identities_match(current, identity):
                raise ProcessStateError("Stop target identity changed while waiting for exit")
            if process.status() in {psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD}:
                # Use a new object: psutil caches create_time on each Process.
                fresh = ProcessIdentity(identity.pid, float(psutil.Process(identity.pid).create_time()))
                if not identities_match(fresh, identity):
                    raise ProcessStateError("Stop target identity changed during exit confirmation")
                reap_children()
                return True
        except (psutil.NoSuchProcess, ProcessLookupError):
            reap_children()
            return True
        except (psutil.Error, OSError) as exc:
            inspection_error = type(exc).__name__
        # argv/cwd can disappear before the kernel reports exit. Neither an empty
        # argv nor an inaccessible observation proves exit or permits escalation.
        # _terminate revalidates full ownership and assignment before any KILL.
        if time.monotonic() >= deadline:
            if inspection_error is not None:
                raise ProcessStateError(f"Exact process exit remains unconfirmed: {inspection_error}")
            return False
        time.sleep(min(0.05, max(0, deadline - time.monotonic())))


def _terminate(root, identity, *, authorization=None, check_assignment=False):
    """Escalate only the same freshly verified own PID, never groups or bots."""
    first_assignment = None
    for signum, timeout in ((signal.SIGTERM, 10), (signal.SIGKILL, 2)):
        state = inspect_pid(root, identity.pid, identity)
        if state["status"] == "stopped":
            return
        if state["status"] != "running":
            raise ProcessStateError(state.get("error", "Process ownership unknown"))
        if authorization is not None:
            fresh = _consume_authorization(root, "stop", authorization)
            if fresh["status"] == "stopped":
                return
        elif check_assignment:
            from pbcoindata_manager import _manual_target
            assignment = _manual_target(state, os.getuid(), time.monotonic() + 5, root=root)
            if first_assignment is not None and assignment != first_assignment:
                raise ProcessStateError("Stop target assignment changed before escalation")
            first_assignment = assignment
        signal_exact_process(identity, signum)
        if _wait_exit(root, identity, timeout):
            return
    raise ProcessStateError("PBCoinData stop remains unconfirmed")


def manager_state(root):
    """Return a fresh manager finding, including positive bounded offline evidence."""
    from pbcoindata_manager import probe_manager
    return probe_manager(root)


class ManualAuthorization:
    """A request-local capability, never a persisted manager or recovery override."""

    def __init__(self, root, action, route, finding, state):
        self.root = Path(root).resolve()
        self.action, self.route, self.finding = action, route, finding
        self.thread = threading.get_ident()
        self.uid = os.getuid()
        self.active = True
        info = self.root.stat()
        self.checkout = info.st_dev, info.st_ino
        self.identity = state.get("identity") if state else None
        self.namespaces = state.get("_manual_namespaces") if state else None


@contextmanager
def manual_action(root, action, *, legacy_target=False):
    """Authorize one explicit action only while its ordered action locks are held."""
    if action not in {"start", "stop"}:
        raise ProcessStateError("Manual restart requires separate stop and start authorization")
    with actions(root, action):
        finding = manager_state(root)
        route = "systemd" if finding["manager"] == "systemd" and not legacy_target else "manual"
        state = _manual_evidence(root, action, finding) if route == "manual" else None
        proof = ManualAuthorization(root, action, route, finding, state)
        try:
            yield proof
        finally:
            proof.active = False


def _manual_evidence(root, action, finding):
    """Check fresh process and namespace evidence without changing manager metadata."""
    from pbcoindata_manager import manual_process_evidence
    state = snapshot(root)
    if state["status"] not in {"running", "stopped"}:
        raise ProcessStateError(state.get("error", "Manual process state is unconfirmed"))
    if action == "start" and state["status"] == "stopped":
        _resolve_launch(root)
    try:
        namespaces = manual_process_evidence(Path(root).resolve(), action, state, finding)
    except Exception as exc:
        _log(SERVICE, f"Manual {action} evidence refused: {type(exc).__name__}", level="WARNING")
        raise ProcessStateError(f"Manual {action} refused: {exc}") from exc
    return {**state, "_manual_namespaces": namespaces}


def _consume_authorization(root, action, proof):
    """A proof cannot cross actions, requests, checkouts, threads or fresh state changes."""
    if (not isinstance(proof, ManualAuthorization) or not proof.active or proof.route != "manual"
            or proof.root != Path(root).resolve() or proof.action != action
            or proof.thread != threading.get_ident() or proof.uid != os.getuid()):
        raise ProcessStateError("An active action-specific manual authorization is required")
    info = Path(root).resolve().stat()
    if proof.checkout != (info.st_dev, info.st_ino):
        raise ProcessStateError("Manual checkout identity changed")
    finding = manager_state(root)
    # A new installed unit changes routing; a target-only migration stop is still
    # allowed only after the same fresh cgroup check below.
    if action == "start" and finding["manager"] == "systemd":
        raise ProcessStateError("Confirmed systemd ownership cannot fall back to legacy")
    state = _manual_evidence(root, action, finding)
    if proof.namespaces != state.get("_manual_namespaces"):
        raise ProcessStateError("Manual namespace evidence changed")
    if action == "stop" and state["status"] == "running" and not identities_match(proof.identity, state.get("identity")):
        raise ProcessStateError("Manual stop target changed")
    return state


def _registration(root):
    """Read protocol publication without repairing stale or damaged evidence."""
    try:
        value = json.loads(guard_protocol.registration_path(root).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    if not isinstance(value, dict) or any(value.get(k) != v for k, v in guard_protocol.scope(root).items()):
        raise ProcessStateError("PBCoinData registration checkout or protocol is unconfirmed")
    return value


def registration_ready(root, identity, *, launch_token=None, observation=None):
    """Require exact ready identity and a currently held matching guard inode."""
    try:
        if observation is not None:
            if not isinstance(observation, PublicationObservation) or observation.root != Path(root).resolve():
                return False
            raw = observation.bundle['files'][guard_protocol.registration_path(root)]
            value = json.loads(raw) if raw is not None else None
        else:
            value = _registration(root)
        if (value is None or value.get("phase") != "ready" or identity is None
                or not identities_match(identity, ProcessIdentity(value["pid"], value["create_time"]))
                or (launch_token is not None and value.get("launch_token") != launch_token)
                or any(value.get(k) != v for k, v in guard_protocol.runtime_scope().items())):
            return False
        if observation is not None:
            return observation.guard.matches(value, identity)
        path = Path(root).resolve() / "data/locks/pbcoindata-singleton.lock"
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            import fcntl
            info = os.fstat(fd)
            if (info.st_dev, info.st_ino) != (value["guard_device"], value["guard_inode"]):
                return False
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                payload = json.loads(os.read(fd, 4096))
                return (payload.get("owner") == "daemon" and payload.get("protocol") == guard_protocol.PROTOCOL
                        and identities_match(identity, ProcessIdentity(payload["pid"], payload["create_time"])))
            return False
        finally:
            os.close(fd)
    except (OSError, ValueError, TypeError, KeyError):
        return False


def ownership_conflicts(root, state):
    """Concrete checkout publications and pending launches supersede global process heuristics."""
    pending = _read_launch(root)
    if pending is not None and _launch_active(pending):
        raise ProcessStateError("Previous PBCoinData start remains unconfirmed")
    for record in guard_protocol.read_publications(root)["records"]:
        observed = _publication_state(root, record)
        if observed == "retired":
            continue
        identity = state.get("identity")
        if (observed != "active" or state.get("status") != "running" or identity is None
                or record["pid"] != identity.pid or (record.get("create_time") is not None
                    and not identities_match(identity, ProcessIdentity(record["pid"], record["create_time"])))):
            raise ProcessStateError("Conflicting or inaccessible PBCoinData publication")


def transition_status(root):
    """Validate independent operator evidence; starting/ready never creates it."""
    evidence = _read_document(root)["transition"]
    if (not isinstance(evidence, dict) or any(evidence.get(k) != v for k, v in guard_protocol.scope(root).items())
            or evidence.get("basis") not in {"legacy-cleanup", "clean-boot"}
            or evidence.get("autostarts_updated") is not True or evidence.get("controllers_restarted") is not True
            or type(evidence.get("confirmed_at")) not in (int, float)
            or not math.isfinite(evidence["confirmed_at"]) or evidence["confirmed_at"] <= 0):
        raise ProcessStateError("Guard transition has not been independently confirmed for this checkout")
    return evidence


def confirm_guard_transition(root, basis):
    """Record a separately completed operator transition; never perform cleanup or reboot."""
    if basis not in {"legacy-cleanup", "clean-boot"}:
        raise ProcessStateError("A documented transition basis is required")
    with actions(root, "confirm-guard-transition"):
        state = snapshot(root)
        if state["status"] != "stopped":
            raise ProcessStateError("Transition confirmation requires a confirmed stopped checkout daemon")
        ownership_conflicts(root, state)
        document = _read_document(root)
        if document["pending_launch"] is not None:
            raise ProcessStateError("Pending launch prevents transition confirmation")
        probe = FileLease(root, "singleton", "transition-probe")
        probe.release()
        document["transition"] = {**guard_protocol.scope(root), **guard_protocol.runtime_scope(),
                                  "basis": basis, "autostarts_updated": True, "controllers_restarted": True,
                                  "confirmed_at": _wall_now()}
        guard_protocol.write_journal(root, document)


def _start_path_ready(root, *, check_interpreter=True):
    """Read current execution/read permissions without predicting future environment changes."""
    root = Path(root).resolve()
    return ((not check_interpreter or (Path(sys.executable).is_file() and os.access(sys.executable, os.X_OK)))
            and all((root / name).is_file() and os.access(root / name, os.R_OK)
                    for name in ("PBCoinData.py", "pbcoindata_guard.py", "secure_files.py")))


def restart_preflight(root):
    """Reject already visible start obstacles before an avoidable manual stop."""
    with actions(root, "restart-preflight"):
        finding = manager_state(root)
        if finding["manager"] == "systemd":
            systemd_start_preflight(root)
            return  # systemd retains its own checked route; never fall back.
        _manual_evidence(root, "start", finding)
        root = Path(root).resolve()
        if not _start_path_ready(root):
            raise ProcessStateError("PBCoinData guarded start path is unavailable")


def _systemd_entry(root, value):
    """Validate the effective entry even when a unit has no process yet."""
    if not _start_path_ready(root, check_interpreter=False):
        raise ProcessStateError("PBCoinData guarded systemd entry is unavailable")
    root = Path(root).resolve()
    match = re.fullmatch(r"\{ path=([^ ;]+) ; argv\[\]=(.+?) ; ignore_errors=(?:yes|no) ;.*\}", value["ExecStart"])
    if (not Path(value["WorkingDirectory"]).is_absolute()
            or Path(value["WorkingDirectory"]).resolve() != root
            or match is None or value["ExecStart"].count("{") != 1 or "\\" in match[2]):
        raise ProcessStateError("PBCoinData systemd checkout or entry is unconfirmed")
    interpreter = Path(match[1])
    argv = shlex.split(match[2])
    args = argv[1:]
    if args[:1] == ["-u"]:
        args = args[1:]
    if (not interpreter.is_absolute() or not interpreter.is_file() or not os.access(interpreter, os.X_OK)
            or not argv or Path(argv[0]).resolve() != interpreter.resolve() or len(args) != 1
            or (root / args[0]).resolve() != root / "PBCoinData.py"):
        raise ProcessStateError("PBCoinData effective systemd interpreter/script is unconfirmed")
    return interpreter


def _observed_records(observation, identity):
    """Reject concrete conflicting publications without interpreting error text."""
    active = []
    for record in observation.bundle['records']:
        classification = _publication_state(observation.root, record)
        if classification == 'retired': continue
        if (classification != 'active' or identity is None or record['pid'] != identity.pid
                or not identities_match(identity, ProcessIdentity(record['pid'],record.get('create_time',0)))):
            raise ProcessStateError('Conflicting PBCoinData publication during observation')
        active.append(record)
    return active


def _guard_payload(guard, identity, context):
    """Only known own-write prefixes or unchanged preflight residue can be awaited."""
    if guard.holder is not None and not identities_match(guard.holder, identity):
        raise ProcessStateError('A foreign granted singleton prevents systemd observation')
    baseline = context.before['_observation'].guard if context is not None else None
    if guard.holder is None:
        if context is not None and guard.payload not in (b'', baseline.payload):
            raise ProcessStateError('Unheld singleton payload changed without a granted daemon lock')
        return False
    expected = json.dumps({'owner':'daemon','protocol':guard_protocol.PROTOCOL,
                           'pid':identity.pid,'create_time':identity.create_time}).encode()
    try:
        payload = json.loads(guard.payload)
    except ValueError:
        if context is not None and (expected.startswith(guard.payload) or guard.payload == baseline.payload):
            return False
        raise ProcessStateError('Singleton payload is incomplete outside the authorized start')
    if (isinstance(payload,dict) and payload.get('owner') == 'daemon'
            and payload.get('protocol') == guard_protocol.PROTOCOL
            and identities_match(identity, ProcessIdentity(payload.get('pid'),payload.get('create_time')))):
        return True
    if context is not None and guard.payload == baseline.payload:
        return False
    raise ProcessStateError('Singleton payload contradicts the current daemon identity')


def _lifetime_ended(identity):
    """Confirm exact exit without cmdline assumptions, signals or foreign reaping."""
    try:
        process = psutil.Process(identity.pid)
        if not identities_match(identity, ProcessIdentity(identity.pid,process.create_time())):
            raise ProcessStateError('Previous PBCoinData PID was recycled')
        terminal = process.status() in {psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD}
        if not identities_match(identity, ProcessIdentity(identity.pid,psutil.Process(identity.pid).create_time())):
            raise ProcessStateError('Previous PBCoinData lifetime changed')
        return terminal
    except psutil.NoSuchProcess:
        return True


def _progress_allowed(root, context, progress):
    """Retry only typed own progress; uncertainty while validating it stays Unknown."""
    try:
        return (context is not None and context.valid(root) and context.invocation is not None
                and all(identities_match(owner,context.identity) for owner in progress.holders)
                and (progress.exited_pid is None or not context.starting and context.identity is not None
                     and progress.exited_pid == context.identity.pid and _lifetime_ended(context.identity)))
    except (OSError, ValueError, ProcessStateError, psutil.Error):
        return False


def systemd_observation(root, deadline=None, *, context=None):
    """Observe a coherent unit/lock/publication sample without acquiring any flock."""
    from pbcoindata_manager import _query_unit, _target_groups, _within_group, _check, systemd_idle_evidence
    root = Path(root).resolve()
    deadline = time.monotonic()+5 if deadline is None else deadline
    result = {'running':False,'actual_status':'unknown','manager':'systemd','unit':UNIT,
              'can_stop':False,'error':'PBCoinData systemd identity is unconfirmed'}
    identity = None
    observation_locked = False
    try:
        if context is None:
            with _SYSTEMD_ATTEMPTS_LOCK:
                candidate = _SYSTEMD_ATTEMPTS.get(root)
                if candidate is not None and candidate.valid(root): context = candidate
        if context is not None: deadline = min(deadline,context.deadline)
        if context is not None and not context.valid(root):
            raise ProcessStateError('Systemd observation context expired or changed')
        if context is not None:
            if not context.lock.acquire(timeout=max(0,deadline-time.monotonic())):
                raise ProcessStateError('Coherent systemd observation exceeded its remaining budget')
            observation_locked = True
            if not context.valid(root):
                raise ProcessStateError('Systemd observation context expired while waiting')
        value = _query_unit(UNIT,deadline,allow_inactive=True)
        result.update(systemd_state=value['ActiveState'],systemd_substate=value['SubState'],_unit=value)
        interpreter = _systemd_entry(root,value)
        pid = int(value['MainPID'])
        if (pid > 1 or value['ActiveState'] == 'activating') and (
                not re.fullmatch('[0-9a-f]{32}', value['InvocationID']) or value['InvocationID'] == '0'*32):
            raise ProcessStateError('Systemd InvocationID is unconfirmed')
        stopping = context is not None and not context.starting
        ended = stopping and context.identity is not None and _lifetime_ended(context.identity)
        already_stopped = (stopping and context.identity is None
                           and context.before['actual_status'] in {'stopped', 'failed'})
        if pid > 1:
            if stopping and ended:
                identity = context.identity
                if pid != identity.pid:
                    raise ProcessStateError('Stop observed a different MainPID after exit')
            else:
                try:
                    identity = ProcessIdentity(pid,psutil.Process(pid).create_time())
                except psutil.NoSuchProcess as exc:
                    raise SampleProgress('MainPID exited during observation',exited_pid=pid) from exc
            if stopping:
                if not identities_match(context.identity,identity):
                    raise ProcessStateError('Stop observed a different PBCoinData lifetime')
            elif (inspect_pid(root,pid,identity)['status'] != 'running'
                    or Path(psutil.Process(pid).exe()).resolve() != interpreter.resolve()
                    or not value['ControlGroup'].endswith('/'+UNIT)
                    or not _within_group(_target_groups(pid,deadline),value['ControlGroup'])):
                raise ProcessStateError('PBCoinData MainPID or ControlGroup is unconfirmed')
        elif pid != 0:
            raise ProcessStateError('Systemd MainPID is invalid')
        if context is not None:
            # A completed stop can clear InvocationID, but never adopt a new one.
            if already_stopped and pid == 0:
                if value['InvocationID'] != context.before['_unit']['InvocationID']:
                    raise ProcessStateError('Stopped unit InvocationID changed')
            elif not (stopping and ended and pid == 0 and value['InvocationID'] in {'','0'*32}):
                context.bind(root,value,identity)
        guard = read_systemd_guard(root,deadline)
        observation = _collect_observation(root,guard)
        records = _observed_records(observation, context.identity if stopping else identity)
        if context is not None:
            with context.lock:
                old_inode = context.guard_inode or context.before['_observation'].guard.inode
                if old_inode is not None and guard.inode != old_inode:
                    raise ProcessStateError('Singleton guard inode changed during observation')
                if guard.inode is not None:
                    context.guard_inode = guard.inode
        complete = _guard_payload(guard,context.identity if stopping else identity,context)
        state = snapshot(root,observation=observation)
        result.update(_observation=observation,_guard=guard.inode,_identity=identity)
        phase = None
        if stopping:
            if (ended or already_stopped) and pid == 0 and guard.holder is None and not records and value['ActiveState'] in {'inactive','failed'}:
                actual = 'failed' if value['ActiveState'] == 'failed' else 'stopped'
            elif value['ActiveState'] in {'active','deactivating','inactive','failed'}:
                actual = 'stopping'
            else:
                raise ProcessStateError('Stop unit transition is unconfirmed')
        elif pid > 1:
            if value['ActiveState'] not in {'active', 'activating'}:
                raise ProcessStateError('PBCoinData systemd initialization failed')
            if state['status'] == 'running' and complete:
                if (not identities_match(identity,state.get('identity'))
                        or not registration_ready(root,identity,observation=observation)):
                    raise ProcessStateError('PBCoinData ready guard does not match MainPID')
                systemd_idle_evidence(root,state,deadline,observation=observation)
                actual = 'running'
                result['can_stop'] = True
            elif any(r.get('phase') == 'ready' for r in records):
                raise ProcessStateError('Ready registration contradicts process publications')
            elif complete and any(r.get('phase') == 'starting' for r in records):
                actual = 'starting'
                result['can_stop'] = True  # Full existing ownership, not an Awaiting permission.
            elif context is not None and context.starting and value['ActiveState'] in {'active','activating'}:
                actual, phase = 'starting', Awaiting.PUBLICATION if complete else Awaiting.GUARD
            else:
                raise ProcessStateError('PBCoinData initialization lacks a bound guarded publication')
        elif guard.holder is not None or records:
            raise ProcessStateError('PBCoinData unit/process ownership conflicts')
        elif value['ActiveState'] in {'inactive','failed'} and value['Job'] in {'','0'}:
            actual = 'failed' if value['ActiveState'] == 'failed' else 'stopped'
        elif context is not None and context.starting and value['ActiveState'] == 'activating':
            actual, phase = 'starting', Awaiting.UNIT
        else:
            raise ProcessStateError('PBCoinData unit transition is unconfirmed')
        fresh = _query_unit(UNIT,deadline,allow_inactive=True)
        keys = ('Id','ExecStart','WorkingDirectory','MainPID','ControlGroup','InvocationID')
        if any(fresh[k] != value[k] for k in keys):
            # MainPID may first appear, or disappear after the exact Stop target ended.
            stable_entry = all(fresh[k] == value[k] for k in ('Id','ExecStart','WorkingDirectory'))
            stable = stable_entry and fresh['InvocationID'] == value['InvocationID']
            if (context is not None and stopping and stable_entry and context.identity is not None
                    and int(fresh['MainPID']) == 0 and fresh['InvocationID'] in {context.invocation,'','0'*32}
                    and _lifetime_ended(context.identity)):
                raise SampleProgress('Exact Stop lifetime ended during observation')
            if context is not None and stable:
                if context.starting and pid == 0 and int(fresh['MainPID']) > 1 and context.identity is None:
                    raise SampleProgress('MainPID appeared in the bound invocation')
            raise ProcessStateError('PBCoinData unit assignment changed during observation')
        if fresh['ActiveState'] != value['ActiveState'] or fresh['SubState'] != value['SubState']:
            if context is not None and fresh['InvocationID'] == value['InvocationID']:
                raise SampleProgress('Unit state advanced within the bound invocation')
            raise ProcessStateError('PBCoinData unit state changed during observation')
        if (_publication_bytes(root) != observation.bundle['files']
                or guard_protocol._read_publication_file(guard_protocol.journal_path(root),root) != observation.journal):
            raise SampleProgress('Publications advanced during the bound observation')
        fresh_guard = read_systemd_guard(root,deadline)
        if fresh_guard != guard:
            if guard.inode is not None and fresh_guard.inode != guard.inode:
                raise ProcessStateError('Singleton guard inode changed during final observation')
            if fresh_guard.holder is not None and not identities_match(fresh_guard.holder,identity or (context.identity if stopping else None)):
                raise ProcessStateError('A foreign granted singleton replaced the current observation')
            raise SampleProgress('Own guard advanced during the bound observation')
        if identity is not None and not stopping and not identities_match(identity,inspect_pid(root,pid,identity).get('identity')):
            raise ProcessStateError('PBCoinData lifetime changed during observation')
        _check(deadline)
        if context is not None and not context.valid(root):
            raise ProcessStateError('Systemd observation context ended during the sample')
        result.update(actual_status=actual,running=actual=='running',_phase=phase,
                      error='PBCoinData systemd initialization failed' if actual=='failed' else '',
                      reason='PBCoinData guard readiness pending' if actual=='starting' else '')
    except SampleProgress as exc:
        if _progress_allowed(root,context,exc):
            result.update(actual_status='starting' if context.starting else 'stopping',
                          can_stop=False,error='',_phase=Awaiting.SAMPLE)
        else:
            result['error'] = 'PBCoinData observation changed without a bound attempt'
    except (OSError,ValueError,KeyError,TypeError,psutil.Error,subprocess.SubprocessError,ProcessStateError) as exc:
        result.update(can_stop=False,error=str(exc)[:240] or 'PBCoinData readiness observation failed')
        _log(SERVICE,f'Systemd readiness unavailable: {type(exc).__name__}',level='WARNING')
    finally:
        if observation_locked:
            context.lock.release()
    return result


def systemd_start_preflight(root):
    """Read-only conflict checks accept only a stopped or exactly ready own unit."""
    state = systemd_observation(root)
    if state['actual_status'] not in {'stopped','failed','running'}:
        raise ProcessStateError(state.get('error') or state.get('reason') or 'PBCoinData start is unconfirmed')
    return state


def wait_systemd_transition(root, *, running, previous_identity=None, context=None):
    """All early phases share the authorization-owned absolute ten-second deadline."""
    deadline = context.deadline if context is not None else time.monotonic()+10
    while True:
        state = systemd_observation(root,deadline,context=context)
        actual = state['actual_status']
        if running and actual == 'running':
            if previous_identity is not None and identities_match(previous_identity,state.get('_identity')):
                raise ProcessStateError('PBCoinData restart retained the previous lifetime')
            return state
        if not running and actual in {'stopped','failed'}:
            if previous_identity is not None and not _lifetime_ended(previous_identity):
                raise ProcessStateError('Previous PBCoinData lifetime has not confirmed its end')
            return state
        if time.monotonic() >= deadline:
            raise ProcessStateError('PBCoinData guard readiness timed out after 10 seconds')
        if context is None or actual not in {'starting','stopping'}:
            raise ProcessStateError(state.get('error') or 'PBCoinData did not reach guarded readiness')
        time.sleep(min(.05,max(0,deadline-time.monotonic())))


def systemd_action(root, action, authorization, invoke):
    """Share transient read-only observation, never ownership, across API and Starter."""
    requested = 'start' if action in {'start','enable'} else 'stop'
    root = Path(root).resolve()
    if (not isinstance(authorization,ManualAuthorization) or not authorization.active
            or authorization.route != 'systemd' or authorization.root != root or authorization.action != requested
            or authorization.thread != threading.get_ident() or authorization.uid != os.getuid()
            or manager_state(root)['manager'] != 'systemd'):
        raise ProcessStateError('Current systemd action authorization is required')
    info = root.stat()
    if authorization.checkout != (info.st_dev,info.st_ino):
        raise ProcessStateError('Systemd action checkout identity changed')
    before = systemd_start_preflight(root) if requested == 'start' else systemd_observation(root)
    if before['actual_status'] not in ({'running','stopped','failed'} if requested=='start' else {'running','starting','stopped','failed'}):
        raise ProcessStateError(before.get('error') or 'Systemd target ownership is unconfirmed')
    if requested == 'stop' and before['actual_status']=='starting' and not before['can_stop']:
        raise ProcessStateError('Awaiting does not authorize Stop')
    result = invoke(action)
    if result is None or result.get('action_failed') or result.get('error'):
        raise ProcessStateError('PBCoinData systemd command failed; no legacy fallback')
    attempt = SystemdAttempt(authorization,time.monotonic()+10,before,requested=='start')
    if requested == 'stop':
        attempt.invocation = before['_unit']['InvocationID']
        attempt.identity = before.get('_identity')
        if attempt.identity is not None:
            attempt.assignment = tuple(before['_unit'][k] for k in ('Id','ExecStart','WorkingDirectory','ControlGroup'))
    with _SYSTEMD_ATTEMPTS_LOCK:
        _SYSTEMD_ATTEMPTS[root] = attempt
    try:
        state = wait_systemd_transition(root,running=requested=='start',previous_identity=before.get('_identity') if requested=='stop' else None,context=attempt)
        state.update({k:result[k] for k in ('enabled','can_enable','systemd_enabled_state') if k in result})
        return state
    finally:
        attempt.active = False
        with _SYSTEMD_ATTEMPTS_LOCK:
            if _SYSTEMD_ATTEMPTS.get(root) is attempt: del _SYSTEMD_ATTEMPTS[root]


def unload_preflight(root, decision, state, *, manager, recovery, restart_check=None):
    """Read-only structural recovery assessment, called under controller/action locks."""
    deadline = time.monotonic() + 5
    try:
        if (decision.get("role") != "slave" or decision.get("demand") != "IDLE"
                or not recovery.get("recovery_ready")):
            raise ProcessStateError("Explicit slave IDLE and verified recovery ownership are required")
        from pbgui_purefunc import load_ini_snapshot
        from pbcoindata_controller import enabled
        cfg = load_ini_snapshot(Path(root) / "pbgui.ini").parser
        if cfg.get("main", "role", fallback="").strip().lower() != "slave" or not enabled(cfg):
            raise ProcessStateError("Automatic slave control is not enabled")
        if manager not in {"legacy", "systemd"} or manager_state(root)["manager"] != manager:
            raise ProcessStateError("Manager route is no longer confirmed")
        ownership_conflicts(root, state)
        identity = state.get("identity")
        if state.get("status") != "running" or not registration_ready(root, identity):
            raise ProcessStateError("Running daemon guard/ready identity is unconfirmed")
        if not _start_path_ready(root, check_interpreter=manager == "legacy"):
            raise ProcessStateError("Guarded restart path is unavailable")
        if manager == "legacy":
            transition_status(root)
            retry = inspect_retry(root)
            if retry["blocked"]:
                raise ProcessStateError(f"Start retry deferred until UTC epoch {retry['value']['next_retry']:.0f}")
            reason = retry["reason"]
        else:
            from pbcoindata_manager import systemd_idle_evidence
            systemd_idle_evidence(root, state, deadline)
            if restart_check is None:
                raise ProcessStateError("Systemd restart policy preflight is unavailable")
            allowed, reason = restart_check()
            if not allowed:
                raise ProcessStateError(reason)
            reason = "Confirmed systemd recovery route"
        if time.monotonic() >= deadline:
            raise ProcessStateError("Recovery evidence exceeded five seconds")
        return {"allowed": True, "reason": reason}
    except (OSError, ValueError, TypeError, ProcessStateError, psutil.Error, subprocess.SubprocessError) as exc:
        return {"allowed": False, "reason": str(exc)}


def _retry_path(root):
    """Keep controller-shared retry metadata private and outside PID publications."""
    return Path(root).resolve() / "data" / "state" / "pbcoindata-start.json"


def _launch_path(root):
    """Keep an unresolved launch separate from repairable retry deadlines."""
    return Path(root).resolve() / "data/state/pbcoindata-launch.json"


def _read_document(root):
    """Translate protocol failures into the shared fail-closed action error."""
    try:
        return guard_protocol.read_journal(root)
    except (OSError, ValueError, TypeError) as exc:
        raise ProcessStateError("Previous PBCoinData launch metadata is unconfirmed") from exc


def _read_launch(root):
    """Read only pending launch data, retaining independently recorded transition data."""
    return _read_document(root)["pending_launch"]


def _write_launch(root, value):
    """Merge one reservation into the durable journal under action locks."""
    document = _read_document(root)
    document["pending_launch"] = value
    path = _launch_path(root)
    ensure_private_directory(path.parent)
    atomic_write_private_text(path, json.dumps(document, indent=4) + "\n")
    _sync_launch_directory(root)


def _sync_launch_directory(root):
    """Keep directory persistence local to the ownership journal."""
    guard_protocol.sync_directories(root, _launch_path(root).parent)


def _launch_active(launch):
    """Only exact parent wait status or explicit process exit releases a pending launch."""
    with _CHILDREN_LOCK:
        for child in _CHILDREN.values():
            if getattr(child, "_pbgui_launch_token", None) == launch["token"]:
                return child.poll() is None
    pid = launch["pid"]
    if pid is None:
        return True  # Parent may have died between fork and recording the child PID.
    try:
        process = psutil.Process(pid)
        expected = launch.get("create_time")
        if expected is not None and not identities_match(ProcessIdentity(pid, expected), ProcessIdentity(pid, process.create_time())):
            raise ProcessStateError("Pending PBCoinData launch identity changed")
        return process.status() != psutil.STATUS_ZOMBIE
    except (psutil.NoSuchProcess, ProcessLookupError):
        return False
    except (psutil.Error, OSError, ValueError) as exc:
        raise ProcessStateError("Pending PBCoinData launch exit is unconfirmed") from exc


def _clear_launch(root, token):
    """Clear only the same attempt under the caller's action locks."""
    current = _read_launch(root)
    if current is not None:
        if current["token"] != token:
            raise ProcessStateError("PBCoinData launch marker changed")
        document = _read_document(root)
        document["pending_launch"] = None
        guard_protocol.write_journal(root, document)


def _resolve_launch(root):
    """Retry dates and manual overrides never override an unconfirmed live child."""
    launch = _read_launch(root)
    if launch is not None:
        if _launch_active(launch):
            raise ProcessStateError("Previous PBCoinData start remains unconfirmed")
        _clear_launch(root, launch["token"])


def _write_retry(root, value):
    """The caller holds the shared action locks across read/modify/replace."""
    path = _retry_path(root)
    ensure_private_directory(path.parent)
    atomic_write_private_text(path, json.dumps(value, indent=4) + "\n")


def _wall_now():
    """Reject unusable wall clocks rather than opening an uncontrolled retry loop."""
    now = time.time()
    if not math.isfinite(now) or now <= 0:
        raise ProcessStateError("Wall clock is unavailable for start retry control")
    return now


def _validate_retry(value, now):
    """Use one content policy for read-only preflight and ACTIVE repair."""
    valid = (type(value) is dict and type(value.get("version")) is int and value["version"] == 1
             and type(value.get("failures")) is int and 0 <= value["failures"] <= 32
             and all(type(value.get(k)) in (int, float) and math.isfinite(value[k]) and value[k] >= 0
                     for k in ("last_failure", "next_retry")))
    if not valid:
        raise ValueError("Invalid retry state")
    pending = value.get("attempt_at", 0)
    if pending:
        if (type(pending) not in (int, float) or not math.isfinite(pending) or not 0 < pending <= now
                or value["next_retry"] != pending + 90 or value["last_failure"] > pending):
            raise ValueError("Unconfirmed start attempt or corrected wall clock")
    elif value["failures"] == 0:
        repair = value.get("repair_at", 0)
        if (type(repair) not in (int, float) or not math.isfinite(repair) or not 0 <= repair <= now
                or value["last_failure"] != 0 or value["next_retry"] != (repair + 60 if repair else 0)):
            raise ValueError("Invalid successful retry state")
    elif not (0 < value["last_failure"] <= now and value["last_failure"] + 60 <= value["next_retry"] <= value["last_failure"] + 1800):
        raise ValueError("Retry timestamps are inconsistent or clock moved backwards")
    return value


def inspect_retry(root):
    """Purely read retry state; repairable content is not a structural IDLE veto."""
    now = _wall_now()
    try:
        value = _validate_retry(json.loads(_retry_path(root).read_text(encoding="utf-8")), now)
        return {"value": value, "repair": False, "blocked": now < value["next_retry"], "reason": "Start retry deadline"}
    except FileNotFoundError:
        return {"value": {"version": 1, "failures": 0, "last_failure": 0, "next_retry": 0},
                "repair": False, "blocked": False, "reason": "No previous start failure"}
    except (ValueError, TypeError, KeyError):
        return {"value": {"version": 1, "failures": 0, "last_failure": 0, "next_retry": now + 60, "repair_at": now},
                "repair": True, "blocked": False, "reason": "Retry content needs repair at ACTIVE start"}
    except OSError as exc:
        raise ProcessStateError("Start retry state is not safely readable") from exc


def retry_status(root):
    """Repair under action locks only at ACTIVE start, with the existing minimum wait."""
    result = inspect_retry(root)
    value = result["value"]
    if result["repair"] or not _retry_path(root).exists():
        try:
            _write_retry(root, value)
        except OSError as exc:
            raise ProcessStateError("Start retry state is not safely writable") from exc
    return value


def _start_failed(root):
    """Count only an actual spawn/readiness failure, shared across all controllers."""
    value = retry_status(root)
    now = _wall_now()
    failures = min(32, value["failures"] + 1)
    _write_retry(root, {"version": 1, "failures": failures, "last_failure": now,
                        "next_retry": now + min(1800, 60 * 2 ** min(failures - 1, 5))})


def stop(root, *, authorization=None):
    """Stop only a freshly verified own legacy process under action locks."""
    with actions(root, "stop"):
        if authorization is not None:
            state = _consume_authorization(root, "stop", authorization)
        else:
            finding = manager_state(root)
            if finding["manager"] != "legacy":
                raise ProcessStateError(finding["reason"])
            state = snapshot(root)
        if state["status"] == "stopped":
            return
        if state["status"] != "running":
            raise ProcessStateError(state.get("error", "Process ownership unknown"))
        identity = state["identity"]
        write_process_identity(pid_path(root), identity)
        if authorization is None:
            _terminate(root, identity, check_assignment=True)
        else:
            _terminate(root, identity, authorization=authorization)
        clear_owned_files(root, identity)
        reap_children()


def start(root, *, automatic=False, authorization=None):
    """Start at most one child and confirm publication within ten seconds."""
    root = Path(root).resolve()
    with actions(root, "start"):
        manual = authorization is not None
        if manual:
            if automatic:
                raise ProcessStateError("Automatic starts cannot use manual authorization")
            _consume_authorization(root, "start", authorization)
        elif manager_state(root)["manager"] != "legacy":
            raise ProcessStateError("Legacy start requires confirmed manager evidence")
        state = snapshot(root)
        if state["status"] == "running":
            return
        if state["status"] != "stopped":
            raise ProcessStateError(state.get("error", "Legacy start requires confirmed process exit and absent systemd unit"))
        _resolve_launch(root)
        reap_children()
        try:
            singleton = FileLease(root, "singleton", "start-probe")
        except BlockingIOError as exc:
            raise ProcessStateError("PBCoinData singleton is still owned") from exc
        singleton.release()
        finding = manager_state(root)
        if manual:
            _consume_authorization(root, "start", authorization)
        elif finding["manager"] != "legacy":
            raise ProcessStateError(finding["reason"])
        if snapshot(root)["status"] != "stopped":
            raise ProcessStateError("Process changed before start")
        # A manual success must not grant or reset automatic retry permission.
        retry_before = _retry_path(root).read_bytes() if manual and _retry_path(root).exists() else None
        retry = retry_status(root)
        if automatic and _wall_now() < retry["next_retry"]:
            raise RetryDeferred(f"Start retry deferred until UTC epoch {retry['next_retry']:.0f}")
        # Leave a fail-closed deadline if this parent dies or a later write fails.
        # Readiness (10s) plus cleanup (12s) still leaves at least a 60s wait.
        attempt_at = _wall_now()
        _write_retry(root, {**retry, "attempt_at": attempt_at, "next_retry": attempt_at + 90})
        launch = {"version": 2, "token": uuid.uuid4().hex, "pid": None, "create_time": None,
                  "parent": {"pid": os.getpid(), "create_time": guard_protocol.creation_time()}}
        _write_launch(root, launch)  # No spawn if its uncertainty cannot be persisted.
        child = None
        identity = None
        confirmed = False
        try:
            child = subprocess.Popen([sys.executable, "-u", str(root / "PBCoinData.py")], cwd=root,
                                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                     close_fds=True, start_new_session=True,
                                     env={**os.environ, "PBGUI_COINDATA_LAUNCH_TOKEN": launch["token"]})
            child._pbgui_launch_token = launch["token"]
            with _CHILDREN_LOCK:
                _CHILDREN[child.pid] = child
            launch["pid"] = child.pid
            _write_launch(root, launch)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if child.poll() is not None:
                    raise ProcessStateError("PBCoinData child exited before readiness")
                owned = inspect_pid(root, child.pid)
                if owned["status"] == "running":
                    identity = owned["identity"]
                    if launch["create_time"] is None:
                        launch["create_time"] = identity.create_time
                        _write_launch(root, launch)
                published = snapshot(root)
                if (published["status"] == "running" and identities_match(published.get("identity"), identity)
                        and registration_ready(root, identity, launch_token=launch["token"])):
                    confirmed = True
                    _clear_launch(root, launch["token"])
                    if manual:
                        if retry_before is None:
                            _retry_path(root).unlink(missing_ok=True)
                        else:
                            from secure_files import atomic_write_private_bytes
                            atomic_write_private_bytes(_retry_path(root), retry_before)
                    else:
                        _write_retry(root, {"version": 1, "failures": 0, "last_failure": 0, "next_retry": 0})
                    return
                time.sleep(0.05)
            raise ProcessStateError("PBCoinData startup readiness timed out")
        except BaseException as exc:
            ended = child is None and isinstance(exc, OSError)
            try:
                if child is not None and identity is None and not confirmed:
                    owned = inspect_pid(root, child.pid)
                    if owned["status"] == "running":
                        identity = owned["identity"]
                if identity is not None and not confirmed:
                    _terminate(root, identity)
                    ended = True
                    clear_owned_files(root, identity)
            finally:
                try:
                    if not confirmed and (ended or (child is not None and child.poll() is not None)):
                        _clear_launch(root, launch["token"])
                finally:
                    reap_children()
                    if not confirmed:
                        _start_failed(root)
                    else:
                        _log(SERVICE, "PBCoinData is ready but start completion could not be persisted", level="ERROR")
            raise


@contextmanager
def restart_phase(root, action, requested):
    """Report a failed post-stop phase without disguising the actual stopped state."""
    try:
        yield
    except Exception as exc:
        if action == "restart" and requested == "start":
            try:
                manager = manager_state(root)['manager']
                if manager == 'systemd':
                    observed = systemd_observation(root)
                    actual = {'status': observed['actual_status']}
                elif manager == 'legacy':
                    actual = snapshot(root)
                else:
                    # A lost manager query must not re-enter a mutating legacy probe.
                    guard = read_systemd_guard(root,time.monotonic()+5)
                    actual = snapshot(root,observation=_collect_observation(root,guard))
            except (OSError,ValueError,TypeError,ProcessStateError,psutil.Error,subprocess.SubprocessError) as observation_error:
                actual = {'status':'unknown'}
                _log(SERVICE,f'Restart failure observation unavailable: {type(observation_error).__name__}',level='WARNING')
            if actual.get("status") == "stopped":
                raise ProcessStateError(f"PBCoinData is stopped; restart start phase failed: {exc}") from exc
        raise


def restart(root):
    """Serialize the complete manual restart without acquiring a controller lease."""
    with actions(root, "restart"):
        restart_preflight(root)
        with manual_action(root, "stop") as proof:
            stop(root, authorization=proof)
        with restart_phase(root, "restart", "start"), manual_action(root, "start") as proof:
            start(root, authorization=proof)
