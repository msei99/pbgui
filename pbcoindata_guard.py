"""Stdlib-only PBCoinData lifetime leases and durable ownership protocol."""

from __future__ import annotations

import fcntl
import json
import math
import os
from pathlib import Path
import stat
import re
import uuid

from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow

SERVICE = "PBCoinData"
PROTOCOL = 2
_LIFETIME_GUARDS = []


def process_observation(pid="self"):
    """Read state and lifetime together; comm can itself contain spaces and parentheses."""
    wanted = os.getpid() if pid == "self" else pid
    if not re.fullmatch(r"[1-9][0-9]*", str(wanted)):
        raise ValueError("Invalid process observation PID")
    text = Path(f"/proc/{pid}/stat").read_text()
    opening, closing = text.find("("), text.rfind(")")
    if opening < 1 or closing <= opening or text[:opening].strip() != str(wanted):
        raise ValueError("Process stat PID or comm is malformed")
    fields = text[closing + 1:].split()
    # Current supported Linux stat has 52 fields; fields starts at field 3.
    if (len(fields) < 50 or len(fields[0]) != 1 or fields[0] not in "RSDZTtXxKWPI"
            or any(not re.fullmatch(r"[0-9]+", fields[i]) for i in (11, 12, 13, 14, 19))):
        raise ValueError("Process stat state or time fields are incomplete")
    boot_rows = [row.split() for row in Path("/proc/stat").read_text().splitlines() if row.startswith("btime ")]
    if len(boot_rows) != 1 or len(boot_rows[0]) != 2 or not re.fullmatch(r"[0-9]+", boot_rows[0][1]):
        raise ValueError("Process boot time is unconfirmed")
    rate = os.sysconf("SC_CLK_TCK")
    if type(rate) is not int or rate <= 0:
        raise ValueError("Process clock rate is unconfirmed")
    ticks = int(fields[19])
    try:
        created = int(boot_rows[0][1]) + ticks / rate
    except OverflowError as exc:
        raise ValueError("Process time is out of range") from exc
    if not math.isfinite(created) or created <= 0:
        raise ValueError("Process time is unconfirmed")
    return created, fields[0], ticks


def creation_time(pid="self"):
    """Use psutil-compatible Linux boot-time/ticks without importing psutil."""
    return process_observation(pid)[0]


def scope(root):
    """Bind evidence to this checkout inode, UID and guard protocol epoch."""
    root = Path(root).resolve()
    info = root.stat()
    return {"protocol": PROTOCOL, "checkout": str(root), "device": info.st_dev,
            "inode": info.st_ino, "uid": os.getuid()}


def runtime_scope():
    """Record boot and namespace identity; a reboot never certifies migration."""
    return {"boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "namespaces": {kind: os.readlink(f"/proc/self/ns/{kind}") for kind in ("pid", "mnt", "user", "cgroup")}}


class FileLease:
    """Own the existing stable lock inode with CLOEXEC until explicit release."""

    def __init__(self, root, name, owner):
        self.fd = None
        self.root = Path(root).resolve()
        directory = ensure_private_directory(self.root / "data/locks")
        self.path = directory / f"pbcoindata-{name}.lock"
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        try:
            os.set_inheritable(fd, False)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if not stat.S_ISREG(os.fstat(fd).st_mode) or os.fstat(fd).st_uid != os.getuid():
                raise RuntimeError("Lease inode ownership is unconfirmed")
            os.fchmod(fd, 0o600)
            self.fd = fd
            self.identity = {"pid": os.getpid(), "create_time": creation_time()}
            payload = {"owner": owner, "protocol": PROTOCOL, **self.identity}
            os.ftruncate(fd, 0)
            os.write(fd, json.dumps(payload).encode("utf-8"))
        except BaseException:
            self.fd = None
            os.close(fd)
            raise

    def release(self):
        """Close without unlinking a lock inode or explicitly unlocking fork-shared state."""
        if self.fd is not None:
            fd, self.fd = self.fd, None
            os.close(fd)


def journal_path(root):
    """Return the shared launch/transition document path."""
    return Path(root).resolve() / "data/state/pbcoindata-launch.json"


def read_journal(root):
    """Preserve legacy pending launches without treating them as transition evidence."""
    try:
        value = json.loads(read_regular_file_nofollow(journal_path(root), Path(root).resolve()))
    except FileNotFoundError:
        return {"version": PROTOCOL, "transition": None, "pending_launch": None}
    if type(value) is not dict:
        raise ValueError("PBCoinData ownership journal is corrupt")
    if type(value.get("version")) is not int:
        raise ValueError("Invalid ownership protocol version")
    if value.get("version") == 1:
        value = {"version": PROTOCOL, "transition": None, "pending_launch": value}
    if (type(value.get("version")) is not int or value["version"] != PROTOCOL
            or not {"transition", "pending_launch"} <= value.keys()
            or (value["transition"] is not None and type(value["transition"]) is not dict)):
        raise ValueError("PBCoinData ownership journal version or schema is unconfirmed")
    pending = value["pending_launch"]
    if pending is not None:
        if (type(pending) is not dict or type(pending.get("version")) is not int or pending["version"] not in {1, PROTOCOL}
                or not {"token", "pid", "create_time"} <= pending.keys()
                or not isinstance(pending["token"], str) or len(pending["token"]) != 32
                or any(c not in "0123456789abcdef" for c in pending["token"])
                or (pending["pid"] is not None and (type(pending["pid"]) is not int or pending["pid"] <= 1))
                or (pending["create_time"] is not None and (type(pending["create_time"]) not in (int, float)
                    or not math.isfinite(pending["create_time"]) or pending["create_time"] <= 0))
                or (pending["pid"] is None and pending["create_time"] is not None)):
            raise ValueError("PBCoinData pending launch is corrupt")
        parent = pending.get("parent")
        if (parent is not None or pending["version"] == PROTOCOL) and (type(parent) is not dict or type(parent.get("pid")) is not int or parent["pid"] <= 1
                or type(parent.get("create_time")) not in (int, float)
                or not math.isfinite(parent["create_time"]) or parent["create_time"] <= 0):
            raise ValueError("PBCoinData launch parent identity is corrupt")
    return value


def sync_directories(root, directory):
    """Persist newly created journal/registration ancestors as well as the file entry."""
    for path in (directory, directory.parent, Path(root).resolve()):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def write_journal(root, value):
    """Write only under the caller's ordered action locks."""
    path = journal_path(root)
    ensure_private_directory(path.parent)
    atomic_write_private_text(path, json.dumps(value, indent=4) + "\n")
    sync_directories(root, path.parent)


def registration_path(root):
    """Keep protocol registration separate from compatibility PID publications."""
    return Path(root).resolve() / "data/pid/pbcoindata.registration.json"


def publish(guard, phase, *, expected_files=None):
    """Publish starting/ready only while this process still holds its exact guard inode."""
    if guard.fd is None or guard.identity["pid"] != os.getpid() or phase not in {"starting", "ready"}:
        raise RuntimeError("Daemon guard publication is not authorized")
    info, current = os.fstat(guard.fd), guard.path.stat()
    if (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
        raise RuntimeError("Daemon guard inode changed")
    value = {**scope(guard.root), **runtime_scope(), **guard.identity, "phase": phase,
             "guard_device": info.st_dev, "guard_inode": info.st_ino, "launch_token": guard.launch_token}
    path = registration_path(guard.root)
    if expected_files is not None:
        publications_unchanged(guard.root, expected_files)
    ensure_private_directory(path.parent)
    atomic_write_private_text(path, json.dumps(value, indent=4) + "\n")
    sync_directories(guard.root, path.parent)


def _close_fork_copies():
    """A fork child closes its copies without LOCK_UN on the parent's shared description."""
    for guard in _LIFETIME_GUARDS:
        FileLease.release(guard)
    _LIFETIME_GUARDS.clear()


os.register_at_fork(after_in_child=_close_fork_copies)


def _read_publication_file(path, root):
    """Read exact metadata bytes without following a replaced publication symlink."""
    try:
        return read_regular_file_nofollow(path, Path(root).resolve())
    except FileNotFoundError:
        return None


def _identity_fields(value):
    """Historical identity fields must not accept bools, NaN or coerced strings."""
    if (type(value) is not dict or type(value.get("pid")) is not int or value["pid"] <= 1
            or type(value.get("create_time")) not in (int, float)
            or not math.isfinite(value["create_time"]) or value["create_time"] <= 0):
        raise ValueError("PBCoinData publication identity is corrupt")
    return {"pid": value["pid"], "create_time": value["create_time"]}


def read_publications(root):
    """Keep every historical reference independent; only matching references share identity."""
    pid_path = Path(root).resolve() / "data/pid/pbcoindata.pid"
    sidecar_path = pid_path.with_name(pid_path.name + ".identity.json")
    files = {path: _read_publication_file(path, root) for path in (pid_path, sidecar_path, registration_path(root))}
    records = []
    sidecar = _identity_fields(json.loads(files[sidecar_path])) if files[sidecar_path] is not None else None
    registration = None
    if files[registration_path(root)] is not None:
        value = json.loads(files[registration_path(root)])
        registration = _identity_fields(value)
        if (any(type(value.get(k)) is not type(v) or value[k] != v for k, v in scope(root).items())
                or value.get("phase") not in {"starting", "ready"}
                or not isinstance(value.get("boot_id"), str)
                or str(uuid.UUID(value["boot_id"])) != value["boot_id"]
                or type(value.get("namespaces")) is not dict
                or set(value["namespaces"]) != {"pid", "mnt", "user", "cgroup"}
                or any(not isinstance(v, str) or not re.fullmatch(k + r":\[\d+\]", v)
                       for k, v in value["namespaces"].items())):
            raise ValueError("PBCoinData registration scope is unconfirmed")
        registration.update(boot_id=value["boot_id"], namespaces=value["namespaces"], phase=value["phase"], kind="registration")
    if files[pid_path] is not None:
        text = files[pid_path].decode("utf-8").strip()
        if not text.isdecimal() or int(text) <= 1:
            raise ValueError("PBCoinData numeric PID publication is corrupt")
        numeric = {"pid": int(text), "kind": "pid"}
        if sidecar is not None:
            if numeric["pid"] != sidecar["pid"]:
                raise ValueError("PBCoinData numeric PID and sidecar disagree")
            numeric.update(sidecar)
        if registration is not None and registration["pid"] == numeric["pid"]:
            if sidecar is not None and abs(sidecar["create_time"] - registration["create_time"]) >= 0.01:
                raise ValueError("PBCoinData registration and sidecar disagree")
            numeric.update({k: v for k, v in registration.items() if k != "kind"})
        records.append(numeric)
    elif sidecar is not None:
        records.append({**sidecar, "kind": "sidecar"})
    if registration is not None:
        records.append(registration)
    return {"records": records, "files": files}


def publications_unchanged(root, files):
    """Recheck bytes under the held guard before replacing this process's publications."""
    if any(_read_publication_file(path, root) != data for path, data in files.items()):
        raise RuntimeError("PBCoinData publications changed during registration")


def _local_checkout(root):
    """Boot retirement is meaningful only for a known local checkout filesystem."""
    candidates = []
    root = Path(root).resolve()
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        row = line.split()
        if len(row) < 7 or "-" not in row or "\\" in row[4]:
            return False
        mount = Path(row[4])
        if root == mount or mount in root.parents:
            candidates.append(row)
    if not candidates:
        return False
    row = max(candidates, key=lambda entry: len(Path(entry[4]).parts))
    return row[row.index("-") + 1] in {"ext2", "ext3", "ext4", "xfs", "btrfs", "overlay", "tmpfs"}


def foreign_publication(root, pid, *, details=None):
    """Positively exclude only this referenced process, never globally classify host services."""
    if details is None:
        status = Path(f"/proc/{pid}/status").read_text()
        uid = next(line.split()[1:3] for line in status.splitlines() if line.startswith("Uid:"))
        details = {"uids": tuple(map(int, uid)), "argv": Path(f"/proc/{pid}/cmdline").read_bytes().rstrip(b"\0").decode("utf-8").split("\0"),
                   "cwd": Path(f"/proc/{pid}/cwd").resolve(strict=True), "exe": os.readlink(f"/proc/{pid}/exe")}
    argv = details["argv"]
    if (not argv or not argv[0] or len(details["uids"]) != 2
            or any(not isinstance(arg, str) or any(ord(c) < 32 for c in arg) for arg in argv)):
        raise ValueError("Referenced process invocation is unconfirmed")
    for kind in ("pid", "mnt", "user"):
        if os.readlink(f"/proc/{pid}/ns/{kind}") != os.readlink(f"/proc/self/ns/{kind}"):
            raise ValueError("Referenced process namespace is unconfirmed")
    root = Path(root).resolve()
    cwd = Path(details["cwd"]).resolve()
    script = (root / "PBCoinData.py").resolve()
    try:
        script_info = script.stat()
    except FileNotFoundError:
        script_info = None
    for arg in argv:
        candidate = (Path(arg) if Path(arg).is_absolute() else cwd / arg).resolve()
        if candidate == script:
            return False
        if script_info is not None:
            try:
                info = candidate.stat()
            except FileNotFoundError:
                continue
            if (info.st_dev, info.st_ino) == (script_info.st_dev, script_info.st_ino):
                return False  # Includes hardlinks, symlinks and foreign-UID checkout competitors.
    if re.fullmatch(r"python(?:\d+(?:\.\d+)*)?", Path(details["exe"]).name):
        args = argv[1:]
        if args[:1] == ["-u"]:
            args = args[1:]
        if not args or args[0].startswith("-"):
            raise ValueError("Referenced Python execution has no verifiable script")
    return True


def publication_state(root, record, *, current_time=creation_time, foreign=None):
    """Retirement applies only to old publications, never a signal target or launch fence."""
    try:
        context = runtime_scope()
        old_boot = False
        if record.get("boot_id") is not None:
            if record["boot_id"] != context["boot_id"]:
                if not _local_checkout(root):
                    return "unknown"
                old_boot = True
            if record["namespaces"] != context["namespaces"]:
                return "unknown"
        observed = current_time(record["pid"])
        if type(observed) not in (int, float) or not math.isfinite(observed) or observed <= 0:
            return "unknown"
        expected = record.get("create_time")
        if expected is not None and record.get("boot_id") is not None and not old_boot and abs(observed - expected) < 0.01:
            first = process_observation(record["pid"])
            if abs(first[0] - observed) >= 0.01:
                return "unknown"
            fresh = process_observation(record["pid"])
            if fresh[0] != first[0] or fresh[2] != first[2] or runtime_scope() != context:
                return "unknown"
            terminal = first[1] in {"Z", "X"}
            if terminal != (fresh[1] in {"Z", "X"}):
                return "unknown"
            return "retired" if terminal else "active"
        excluded = (foreign or (lambda pid: foreign_publication(root, pid)))(record["pid"])
        if abs(current_time(record["pid"]) - observed) >= 0.01:
            return "unknown"
        return "retired" if excluded else "active"
    except (FileNotFoundError, ProcessLookupError):
        # Missing procfs fields alone do not prove exit. Confirm against the
        # kernel as well, without delivering a signal (signal 0 is a probe).
        try:
            current_time(record["pid"])
        except (FileNotFoundError, ProcessLookupError):
            try:
                os.kill(record["pid"], 0)
            except ProcessLookupError:
                return "retired"
            except (OSError, ValueError, TypeError):
                return "unknown"
        return "unknown"
    except (OSError, ValueError, KeyError, TypeError, StopIteration):
        return "unknown"


def _check_existing_publications(root):
    """A known retired lifetime does not make its recycled PID a new daemon owner."""
    bundle = read_publications(root)
    for record in bundle["records"]:
        if publication_state(root, record) != "retired":
            raise RuntimeError("Existing live or unresolved PBCoinData publication prevents direct start")
    return bundle["files"]


def _retire_compatibility_files(root, files):
    """Under the lifetime guard, remove only byte-matching, already retired PID metadata."""
    publications_unchanged(root, files)
    for path, content in files.items():
        if path == registration_path(root) or content is None:
            continue
        publications_unchanged(root, files)
        path.unlink()
        files = {**files, path: None}
    return files


def acquire_daemon(root):
    """Acquire before heavy imports and retain the descriptor until actual OS exit."""
    guard = FileLease(root, "singleton", "daemon")
    _LIFETIME_GUARDS.append(guard)
    guard.launch_token = None
    pending = read_journal(root)["pending_launch"]
    if pending is not None:
        token = os.environ.get("PBGUI_COINDATA_LAUNCH_TOKEN")
        parent = pending.get("parent")
        if (pending["version"] != PROTOCOL or token != pending["token"] or not isinstance(parent, dict)
                or parent.get("pid") != os.getppid()
                or abs(creation_time(os.getppid()) - parent.get("create_time", 0)) >= 0.01
                or pending["pid"] not in (None, os.getpid())):
            raise RuntimeError("Unresolved PBCoinData launch prevents this direct start")
        guard.launch_token = token
    previous = _check_existing_publications(root)
    previous = _retire_compatibility_files(root, previous)
    publish(guard, "starting", expected_files=previous)
    return guard
