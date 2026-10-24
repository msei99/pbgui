"""Bounded, conservative evidence for the local PBCoinData service manager."""

from __future__ import annotations

import os
import ctypes
import fcntl
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
import re
import stat
import shlex
import subprocess
import time

import psutil

SERVICE = "PBCoinData"
UNIT = "pbgui-pbcoindata.service"
PF_KTHREAD = 0x00200000  # Linux include/linux/sched.h; proc stat field 9.
PID_NS_INIT_INO = 0xEFFFFFFC  # Linux UAPI linux/nsfs.h; only the initial PID namespace.
CGROUP_NS_INIT_INO = 0xEFFFFFFB  # A private view can hide an outer service cgroup.
USER_NS_INIT_INO = 0xEFFFFFFD
NS_TYPES = {"mnt": 0x00020000, "pid": 0x20000000, "user": 0x10000000, "cgroup": 0x02000000}
NS_GET_NSTYPE = 0xB703  # Linux UAPI linux/nsfs.h; available before Linux 6.8.


def _namespace(pid, kind, deadline):
    """Read an actual namespace descriptor rather than trusting a symlink label."""
    _check(deadline)
    fd = os.open(f"/proc/{pid}/ns/{kind}", os.O_RDONLY | os.O_CLOEXEC)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_ino <= 0
                or fcntl.ioctl(fd, NS_GET_NSTYPE) != NS_TYPES[kind]):
            raise ValueError("Namespace descriptor type is unconfirmed")
        _check(deadline)
        return info.st_dev, info.st_ino
    finally:
        os.close(fd)


def _namespace_metadata(pid, kind, deadline):
    """Require a typed namespace descriptor; a masked symlink's directory stat is not identity."""
    return _namespace(pid, kind, deadline)


def _mount_root(pid, deadline):
    """Identify the visible root mapping, including a possible foreign chroot."""
    roots = []
    for line in _read(f"/proc/{pid}/mountinfo", deadline).splitlines():
        row = line.split()
        if len(row) < 7 or "-" not in row:
            raise ValueError("Process root mapping is incomplete")
        if row[4] == "/":
            if "\\" in row[3]:
                raise ValueError("Process root mapping is ambiguous")
            roots.append((row[0], row[2], row[3]))
    if len(roots) != 1:
        raise ValueError("Process root mapping is unconfirmed")
    return roots[0]


def _cgroup_path(value):
    """Require canonical component boundaries, never substring ownership."""
    if (not isinstance(value, str) or not value.startswith("/") or "\\" in value
            or any(ord(c) < 32 for c in value) or "//" in value
            or any(part in {".", ".."} for part in value.split("/"))
            or (value != "/" and value.endswith("/"))):
        raise ValueError("Cgroup path is malformed")
    return value


def cgroup_membership(text):
    """Parse v1 controller lists and v2 hierarchy with one consistent systemd path."""
    ids, controllers, rows, authoritative = set(), set(), [], []
    for line in text.splitlines():
        fields = line.split(":", 2)
        if len(fields) != 3 or not fields[0].isdecimal():
            raise ValueError("Cgroup hierarchy is malformed")
        hierarchy, names, path = fields
        if int(hierarchy) in ids:
            raise ValueError("Cgroup hierarchy is ambiguous")
        ids.add(int(hierarchy))
        names = names.split(",") if names else []
        if (len(names) != len(set(names)) or any(not re.fullmatch(r"(?:name=)?[A-Za-z0-9_]+", n) for n in names)
                or controllers.intersection(names) or (hierarchy == "0") != (not names)):
            raise ValueError("Cgroup controllers are malformed or ambiguous")
        controllers.update(names)
        path = _cgroup_path(path)
        rows.append(path)
        if not names or "name=systemd" in names:
            authoritative.append(path)
    if not authoritative or len(set(authoritative)) != 1:
        raise ValueError("Systemd cgroup hierarchy is unconfirmed")
    selected = authoritative[0]
    selected_services = tuple(p for p in selected.split("/") if p.endswith(".service"))
    for path in rows:
        services = tuple(p for p in path.split("/") if p.endswith(".service"))
        if services and services != selected_services:
            raise ValueError("Contradictory systemd cgroup hierarchies")
    return selected


def _within_group(path, parent):
    """Compare complete path components, including the root cgroup."""
    return path == parent or parent == "/" or path.startswith(parent + "/")


def _query_unit(unit, deadline, *, user=True, allow_inactive=False, invocation=False):
    """Read one validated unit via argv within the shared five-second evidence budget."""
    if not re.fullmatch(r"[A-Za-z0-9_.:@-]+\.service", unit) or unit.startswith("-"):
        raise ValueError("Unit identifier is invalid")
    _check(deadline)
    properties = ("Id", "LoadState", "ActiveState", "SubState", "MainPID", "ControlGroup", "Job",
                  "ExecStart", "WorkingDirectory", "Result", "StartLimitIntervalUSec", "StartLimitBurst")
    if allow_inactive or invocation:
        properties += ('InvocationID',)
    # Without --all, systemctl omits empty properties such as an idle Job.
    argv = ["systemctl"] + (["--user"] if user else []) + ["show", unit, "--no-pager", "--all"]
    for key in properties:
        argv += ["-p", key]
    result = subprocess.run(argv, capture_output=True, text=True, timeout=min(3, max(0.001, deadline-time.monotonic())))
    _check(deadline)
    if result.returncode:
        raise ValueError("Current unit ownership query is unavailable")
    values = {}
    for line in result.stdout.splitlines():
        key, sep, value = line.partition("=")
        if not sep or key in values:
            raise ValueError("Unit properties are malformed or ambiguous")
        values[key] = value
    if (not set(properties) <= values.keys() or values["Id"] != unit or values["LoadState"] != "loaded"
            or not values["MainPID"].isascii() or not values["MainPID"].isdecimal()
            or (not allow_inactive and (values["ActiveState"] != "active"
                or values["Job"] not in {"", "0"} or int(values["MainPID"]) <= 1))):
        raise ValueError("Current unit ownership is unconfirmed")
    if values["ControlGroup"] or not allow_inactive:
        _cgroup_path(values["ControlGroup"])
    return values


def _target_groups(pid, deadline):
    """Exclude hidden outer ownership and require the same current namespaces."""
    if _namespace("self", "cgroup", deadline)[1] != CGROUP_NS_INIT_INO:
        raise ValueError("Outer systemd target ownership is hidden by the cgroup namespace")
    for kind in ("pid", "mnt", "user", "cgroup"):
        if _namespace(pid, kind, deadline) != _namespace("self", kind, deadline):
            raise ValueError("Target namespace ownership is unconfirmed")
    return cgroup_membership(_read(f"/proc/{pid}/cgroup", deadline))


def _legacy_parent(root, state, main_pid, deadline):
    """Bind every ancestry edge to a lifetime and recheck the complete chain before use."""
    root = Path(root).resolve()

    def observe(pid):
        """Use a fresh psutil object; creation times are cached per instance."""
        _check(deadline)
        process = psutil.Process(pid)
        created = process.create_time()
        uids = process.uids()
        if uids.real != os.getuid() or uids.effective != os.getuid():
            raise ValueError("Legacy parent ownership is unconfirmed")
        argv, executable, cwd = process.cmdline(), process.exe(), process.cwd()
        script = _python_script_argument(argv, {Path(executable).name})
        return (pid, created, process.ppid(), uids.real, uids.effective, tuple(argv), executable, cwd, script)

    current = state["pid"]
    expected_time = state["identity"].create_time
    supported, chain = False, []
    for _ in range(16):
        value = observe(current)
        if value[1] != expected_time:
            raise ValueError("Legacy ancestry lifetime changed")
        if current != state["pid"] and value[8] is not None and Path(value[7]).resolve() == root:
            script = (root / value[8]).resolve()
            if script.parent == root and script.name in {"PBRun.py", "PBApiServer.py", "starter.py", "monitor_agent.py"}:
                supported = True
        chain.append(value)
        if current == main_pid and current != state["pid"]:
            if not supported:
                break
            for previous in chain:
                if observe(previous[0]) != previous:
                    raise ValueError("Legacy ancestry lifetime, invocation or edge changed")
            _check(deadline)
            return tuple((row[0], row[1], row[2], row[8]) for row in chain)
        current = value[2]
        if current <= 1:
            break
        _check(deadline)
        expected_time = psutil.Process(current).create_time()
    raise ValueError("Supported legacy controller ancestry is unconfirmed")


def _manual_target(state, uid, deadline, *, root=None):
    """Distinguish a proven guarded legacy child from a service's actual managed target."""
    if state["status"] == "stopped":
        return None
    pid = state.get("pid")
    group = _target_groups(pid, deadline)
    parts = group.strip("/").split("/")
    services = [(i, part) for i, part in enumerate(parts) if part.endswith(".service")]
    if any(part == UNIT for _, part in services):
        raise ValueError("Target belongs to the PBCoinData systemd unit")
    evidence = []
    if services:
        from pbcoindata_process import inspect_pid, registration_ready
        from process_identity import identities_match
        identity = state.get("identity")
        if (root is None or identity is None or not registration_ready(root, identity)
                or not identities_match(inspect_pid(root, pid, identity).get("identity"), identity)):
            raise ValueError("Supported guarded legacy target identity is unconfirmed")
        user_boundary = next((i for i, part in services if re.fullmatch(r"user@\d+\.service", part)), None)
        for index, unit in services:
            user = user_boundary is not None and index > user_boundary
            value = _query_unit(unit, deadline, user=user)
            expected = "/" + "/".join(parts[:index+1])
            if value["ControlGroup"] != expected or not _within_group(group, value["ControlGroup"]):
                raise ValueError("Target and unit ControlGroup disagree")
            if int(value["MainPID"]) == pid:
                raise ValueError("Target is a systemd service MainPID")
            evidence.append((unit, user, value["MainPID"], value["ControlGroup"]))
        if not identities_match(inspect_pid(root, pid, identity).get("identity"), identity):
            raise ValueError("Target identity changed during unit assignment")
    if _target_groups(pid, deadline) != group:
        raise ValueError("Target cgroup changed during unit assignment")
    origin = None
    if services:
        last_index = services[-1][0]
        direct_scope = (last_index == user_boundary and any(p.endswith(".scope") for p in parts[last_index+1:]))
        if not direct_scope:
            origin = _legacy_parent(root, state, int(evidence[-1][2]), deadline)
        if not identities_match(inspect_pid(root, pid, identity).get("identity"), identity):
            raise ValueError("Legacy target lifetime changed during ancestry inspection")
    if _target_groups(pid, deadline) != group:
        raise ValueError("Target cgroup changed during ancestry inspection")
    _check(deadline)
    return group, tuple(evidence), origin


def systemd_idle_evidence(root, state, deadline, *, observation=None):
    """Bind the actual PBCoinData unit to its exact guarded process and effective entry."""
    from pbcoindata_process import inspect_pid, registration_ready
    from process_identity import identities_match
    identity = state.get("identity")
    if identity is None or not registration_ready(root, identity, **({'observation':observation} if observation is not None else {})):
        raise ValueError("Systemd guard identity is unconfirmed")
    before = inspect_pid(root, identity.pid, identity)
    if before.get("status") != "running":
        raise ValueError("Systemd process identity is unconfirmed")
    group = _target_groups(identity.pid, deadline)
    value = _query_unit(UNIT, deadline, **({'invocation':True} if observation is not None else {}))
    if (int(value["MainPID"]) != identity.pid or value["SubState"] != "running"
            or value["ControlGroup"].split("/")[-1] != UNIT
            or not _within_group(group, value["ControlGroup"])):
        raise ValueError("Systemd MainPID or ControlGroup disagrees with guarded daemon")
    root = Path(root).resolve()
    if (not Path(value["WorkingDirectory"]).is_absolute()
            or Path(value["WorkingDirectory"]).resolve() != root or value["Result"] == "start-limit-hit"):
        raise ValueError("Systemd checkout or start limit is unconfirmed")
    if not value["StartLimitBurst"].isdecimal() or not value["StartLimitIntervalUSec"]:
        raise ValueError("Systemd start restrictions are unavailable")
    match = re.fullmatch(r"\{ path=([^ ;]+) ; argv\[\]=(.+?) ; ignore_errors=(?:yes|no) ;.*\}", value["ExecStart"])
    if match is None or value["ExecStart"].count("{") != 1 or "\\" in match[2]:
        raise ValueError("Effective guarded systemd ExecStart is unconfirmed")
    interpreter = Path(match[1])
    argv = shlex.split(match[2])
    args = argv[1:]
    if args[:1] == ["-u"]:
        args = args[1:]
    if (not interpreter.is_absolute() or not interpreter.is_file() or not os.access(interpreter, os.X_OK)
            or not argv or Path(argv[0]).resolve() != interpreter.resolve()
            or len(args) != 1 or (root / args[0]).resolve() != (root / "PBCoinData.py").resolve()
            or Path(psutil.Process(identity.pid).exe()).resolve() != interpreter.resolve()):
        raise ValueError("Effective guarded systemd interpreter/script is unavailable")
    if (_target_groups(identity.pid, deadline) != group
            or not identities_match(inspect_pid(root, identity.pid, identity).get("identity"), identity)
            or not registration_ready(root, identity, **({'observation':observation} if observation is not None else {}))):
        raise ValueError("Systemd target changed during ownership evidence")
    if _query_unit(UNIT, deadline, **({'invocation':True} if observation is not None else {})) != value:
        raise ValueError("Systemd unit changed during ownership evidence")
    _check(deadline)


@dataclass(frozen=True)
class _Mount:
    """One complete immutable kernel mountinfo record, including its attachment identity."""
    ident: int
    parent: int
    device: tuple
    root: str
    point: str
    options: tuple
    optional: tuple
    filesystem: str
    source: str
    super_options: tuple


def _mount_text(value, *, path=False):
    """Decode only kernel seq_file escapes and require unambiguous absolute mount paths."""
    if re.search(r"\\(?!040|011|012|134|043)", value):
        raise ValueError("Mountinfo escape is malformed")
    value = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), value)
    if (not value or any(ord(c) < 32 for c in value)
            or path and (not value.startswith("/") or "//" in value
                         or any(p in {".", ".."} for p in value.split("/"))
                         or value != "/" and value.endswith("/"))):
        raise ValueError("Mountinfo path or value is ambiguous")
    return value


def _mountinfo(text, deadline):
    """Validate complete mount records and their rooted, acyclic attachment topology."""
    if not text or not text.endswith("\n"):
        raise ValueError("Mountinfo is empty or truncated")
    rows = {}
    for line in text.splitlines():
        _check(deadline)
        fields = line.split()
        if "-" not in fields[6:]:
            raise ValueError("Mountinfo separator is malformed")
        sep = fields.index("-", 6)
        if (sep < 6 or len(fields) != sep + 4 or not fields[0].isdecimal()
                or not fields[1].isdecimal() or not re.fullmatch(r"\d+:\d+", fields[2])
                or not re.fullmatch(r"[A-Za-z0-9_.-]+", fields[sep + 1])):
            raise ValueError("Mountinfo record is incomplete")
        ident, parent = int(fields[0]), int(fields[1])
        if ident <= 0 or ident in rows:
            raise ValueError("Mountinfo IDs are contradictory")
        optional = tuple(fields[6:sep])
        if (len(set(optional)) != len(optional)
                or any(not re.fullmatch(r"(?:shared|master|propagate_from):[1-9]\d*|unbindable", v) for v in optional)):
            raise ValueError("Mountinfo propagation is unconfirmed")
        options, super_options = tuple(fields[5].split(",")), tuple(fields[sep + 3].split(","))
        if any(not option for option in options + super_options):
            raise ValueError("Mountinfo options are incomplete")
        filesystem = fields[sep + 1]
        root = _mount_text(fields[3], path=filesystem != "nsfs")
        if filesystem == "nsfs":
            # nsfs_show_path emits a namespace label, not a filesystem path.
            # Preserve it exactly: it still participates in every full comparison.
            label = re.fullmatch(r"(?:cgroup|ipc|mnt|net|pid|pid_for_children|time|time_for_children|user|uts):\[([1-9][0-9]{0,19})\]", root)
            if label is None or int(label[1]) > 0xFFFFFFFFFFFFFFFF:
                raise ValueError("Mountinfo namespace root label is malformed")
        rows[ident] = _Mount(ident, parent, tuple(map(int, fields[2].split(":"))),
                             root, _mount_text(fields[4], path=True),
                             options, optional, filesystem, _mount_text(fields[sep + 2]), super_options)
    roots = [row for row in rows.values() if row.point == "/"]
    if len(roots) != 1 or roots[0].parent in rows and roots[0].parent != roots[0].ident:
        raise ValueError("Unique complete root mount is unconfirmed")
    for row in rows.values():
        visited, current = set(), row
        while current != roots[0]:
            _check(deadline)
            if current.ident in visited or current.parent not in rows:
                raise ValueError("Mountinfo topology is incomplete or cyclic")
            visited.add(current.ident)
            parent = rows[current.parent]
            if not _within_group(current.point, parent.point):
                raise ValueError("Mountinfo parent mapping is contradictory")
            current = parent
    return tuple(rows[key] for key in sorted(rows))


def _mount_view(rows, root):
    """Require an unrestricted procfs root, only benign binfmt stacks, and a local checkout."""
    proc = [row for row in rows if row.point == "/proc"]
    if (len(proc) != 1 or proc[0].filesystem != "proc" or proc[0].root != "/"
            or any(v.startswith("hidepid=") and v != "hidepid=0" or v.startswith("subset=")
                   for v in proc[0].options + proc[0].super_options)):
        raise ValueError("Complete procfs visibility is unavailable")
    children = [row for row in rows if row.point.startswith("/proc/")]
    if (len(children) > 2 or len({row.filesystem for row in children}) != len(children)
            or any(row.point != "/proc/sys/fs/binfmt_misc" or row.root != "/"
                   or row.filesystem not in {"autofs", "binfmt_misc"} or row.device[0] != 0
                   or row.parent not in {proc[0].ident, *(child.ident for child in children)} for row in children)):
        raise ValueError("Critical or unknown procfs overlays prevent complete visibility")
    points = [row.point for row in rows if row not in children]
    if len(points) != len(set(points)):
        raise ValueError("Stacked mount mapping is ambiguous")
    candidates = [row for row in rows if _within_group(str(root), row.point)]
    checkout = max(candidates, key=lambda row: len(Path(row.point).parts))
    if checkout.filesystem not in {"ext2", "ext3", "ext4", "xfs", "btrfs", "overlay", "tmpfs"}:
        raise ValueError("Checkout may be shared with controllers outside this kernel")
    return proc[0], checkout


def _procfs_fd(fd):
    """Authenticate the file's filesystem through the kernel, independently of its text."""
    libc = ctypes.CDLL(None, use_errno=True)
    query = libc.fstatfs
    query.argtypes, query.restype = [ctypes.c_int, ctypes.c_void_p], ctypes.c_int
    # Linux's native struct statfs fits within 32 native longs on supported
    # ABIs. Only its first native long (f_type) is read; no /proc text is trusted.
    result = (ctypes.c_long * 32)()
    if query(fd, result) != 0:
        raise OSError(ctypes.get_errno(), "Mountinfo filesystem query failed")
    if result[0] != 0x9FA0:  # PROC_SUPER_MAGIC, linux/magic.h.
        raise ValueError("Mountinfo descriptor is not procfs")


def _mountinfo_fd(pid, stack, root, deadline):
    """Pin a procfs observation and its root references until both comparisons finish."""
    _check(deadline)
    fd = os.open(f"/proc/{pid}/mountinfo", os.O_RDONLY | os.O_CLOEXEC)
    stack.callback(os.close, fd)
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("Mountinfo is not a regular procfs file")
    _procfs_fd(fd)
    _check(deadline)
    chunks, count = [], 0
    while True:
        _check(deadline)
        chunk = os.read(fd, min(16384, 262145 - count))
        _check(deadline)
        if not chunk:
            break
        count += len(chunk)
        if count > 262144:
            raise ValueError("Mountinfo exceeds evidence read limit")
        chunks.append(chunk)
    rows = _mountinfo(b"".join(chunks).decode("utf-8"), deadline)
    proc, checkout = _mount_view(rows, root)
    mount_ids = re.findall(r"^mnt_id:\s*(\d+)\s*$", _read(f"/proc/self/fdinfo/{fd}", deadline), re.M)
    if info.st_dev != os.makedev(*proc.device) or mount_ids != [str(proc.ident)]:
        raise ValueError("Mountinfo descriptor is not attached to the uncovered procfs root")
    return rows


def _manual_start_view(root, deadline):
    """Positively bind the complete view to PID 1 without opening its masked namespace links."""
    namespaces = tuple(_namespace_metadata("self", kind, deadline) for kind in ("pid", "mnt", "user"))
    if namespaces[0][1] != PID_NS_INIT_INO or namespaces[2][1] != USER_NS_INIT_INO:
        raise ValueError("Initial PID and user namespace visibility is unconfirmed")
    with ExitStack() as stack:
        observations = []
        for _ in range(2):
            # Fresh opens are essential: old descriptors retain PID 1's old root
            # after a namespace switch. Keep them pinned while comparing new ones.
            current = _mountinfo_fd("self", stack, root, deadline)
            initial = _mountinfo_fd(1, stack, root, deadline)
            if current != initial or observations and current != observations[0]:
                raise ValueError("Root, procfs or checkout mount identity changed or differs from PID 1")
            observations.append(current)
        if namespaces != tuple(_namespace_metadata("self", kind, deadline) for kind in ("pid", "mnt", "user")):
            raise ValueError("Namespace identity changed during mount evidence")
        if (_read("/proc/self/uid_map", deadline).split() != ["0", "0", "4294967295"]
                or re.findall(r"^NSpid:\s*(.+)$", _read("/proc/self/status", deadline), re.M) != [str(os.getpid())]):
            raise ValueError("Complete initial process view is unconfirmed")
        _check(deadline)
        return namespaces, observations[0]


def _python_script_argument(argv, names):
    """Locate Python's script operand, independently of its basename or alias."""
    if not any(re.fullmatch(r"python(?:\d+(?:\.\d+)*)?", name) for name in names):
        return None
    index = 1
    while index < len(argv):
        argument = argv[index]
        if argument in {"-h", "--help", "--help-env", "--help-xoptions", "--help-all", "--version"} or re.fullmatch(r"-V+", argument):
            return None  # These CPython modes exit without executing a script.
        if argument == "--":
            if index + 1 >= len(argv) or argv[index + 1] == "-":
                raise ValueError("Python stdin execution has no verifiable script operand")
            return argv[index + 1]
        if not argument.startswith("-"):
            return argument
        if argument in {"-c", "-m", "-"}:
            raise ValueError("Python execution has no verifiable script operand")
        if argument in {"-W", "-X", "--check-hash-based-pycs"}:
            index += 2
            if index >= len(argv):
                raise ValueError("Python script operand is incomplete")
            continue
        if (re.fullmatch(r"-[bBdEiIOqRsSuvVx]+", argument)
                or argument.startswith(("-W", "-X", "--check-hash-based-pycs="))):
            index += 1
            continue
        raise ValueError("Python invocation cannot exclude a checkout script alias")
    raise ValueError("Python execution has no verifiable script operand")


def _manual_manager_absent(deadline):
    """Inspect only this UID's possible user manager, never classify foreign services."""
    for pid in psutil.pids():
        _check(deadline)
        try:
            process = psutil.Process(pid)
            uids = process.uids()
            if os.getuid() not in (uids.real, uids.effective) or process.status() == psutil.STATUS_ZOMBIE:
                continue
            argv = process.cmdline()
            names = {process.name(), Path(process.exe()).name, Path(argv[0]).name if argv else ""}
            if not argv or ("systemd" in names and ("--user" in argv or pid != 1)):
                raise ValueError("An inaccessible live user manager cannot be excluded")
        except psutil.NoSuchProcess:
            continue


def manual_process_evidence(root, action, state, finding):
    """Authorize one manual action without promoting UNKNOWN to an automatic manager."""
    deadline = time.monotonic() + 5
    if action == "stop":
        target = _manual_target(state, os.getuid(), deadline, root=root)
        return tuple(_namespace("self", kind, deadline) for kind in ("pid", "mnt", "user", "cgroup")) + (target,)
    mount_view = _manual_start_view(root, deadline)
    # Shared guard, exact publications and journal replace global foreign-process
    # classification. Ordinary system services are not ownership evidence.
    from pbcoindata_process import ownership_conflicts
    ownership_conflicts(root, state)
    target = _manual_target(state, os.getuid(), deadline, root=root)
    # A positive manager finding already includes the unit paths and current
    # query. Without it, use configured paths without requiring systemd-analyze.
    if finding["manager"] == "unknown":
        _manual_manager_absent(deadline)
        env = dict(os.environ)
        paths, runtimes = _unit_paths_without_helper(env, Path.home(), os.getuid(), deadline)
        if _artifacts(paths, deadline):
            raise ValueError("Relevant systemd unit artifacts prevent manual start")
        for runtime in runtimes:
            for relative in ("bus", "systemd/private"):
                if _lstat(runtime / relative, deadline) is not None:
                    raise ValueError("A live or inaccessible user manager cannot be excluded")
    _check(deadline)
    return tuple(_namespace("self", kind, deadline) for kind in ("pid", "mnt", "user", "cgroup")) + (target, mount_view)


def _check(deadline):
    """Share one deadline across commands, metadata and process enumeration."""
    if time.monotonic() >= deadline:
        raise TimeoutError("Manager evidence exceeded five seconds")


def _absolute(value):
    """Reject ambiguous environment/load-path entries without exposing their values."""
    if not value or not Path(value).is_absolute() or any(ord(c) < 32 for c in value):
        raise ValueError("Uncertain systemd search path")
    return Path(value)


def _unit_paths_without_helper(env, home, uid, deadline):
    """Collect configured and standard paths without executing a systemd utility."""
    def single(name, default):
        return _absolute(env.get(name) or str(default))

    def multiple(name, default):
        return [_absolute(v) for v in (env.get(name) or default).split(":")]

    home = _absolute(str(home))
    config = single("XDG_CONFIG_HOME", home / ".config")
    data = single("XDG_DATA_HOME", home / ".local/share")
    runtime = single("XDG_RUNTIME_DIR", f"/run/user/{uid}")
    runtimes = {runtime, Path(f"/run/user/{uid}")}
    paths = [config / "systemd/user.control", config / "systemd/user", data / "systemd/user",
             home / ".config/systemd/user", home / ".config/systemd/user.control",
             home / ".local/share/systemd/user"]
    paths += [p / "systemd/user" for p in multiple("XDG_CONFIG_DIRS", "/etc/xdg")]
    paths += [p / "systemd/user" for p in multiple("XDG_DATA_DIRS", "/usr/local/share:/usr/share")]
    paths += [Path(p) for p in ("/etc/systemd/user", "/etc/systemd/user.attached", "/run/systemd/user",
                               "/run/systemd/user.attached", "/usr/local/lib/systemd/user", "/usr/lib/systemd/user", "/lib/systemd/user")]
    for base in runtimes:
        paths += [base / "systemd" / suffix for suffix in
                  ("user.control", "transient", "generator.early", "user", "user.attached", "generator", "generator.late")]
    if "SYSTEMD_UNIT_PATH" in env:
        entries = env["SYSTEMD_UNIT_PATH"].split(":")
        if entries[-1] == "":
            entries.pop()  # A trailing empty entry appends the normal load path.
        paths += [_absolute(v) for v in entries]
    _check(deadline)
    return list(dict.fromkeys(paths)), runtimes


def _unit_paths(env, home, uid, deadline):
    """Include compiled paths and every configured/default user-unit search space."""
    paths, runtimes = _unit_paths_without_helper(env, home, uid, deadline)
    result = subprocess.run(["systemd-analyze", "--user", "unit-paths"], capture_output=True,
                            text=True, env=env, timeout=max(0.001, deadline - time.monotonic()))
    if result.returncode or not result.stdout.strip() or len(result.stdout) > 65536:
        raise ValueError("Compiled unit search paths unavailable")
    paths += [_absolute(line) for line in result.stdout.splitlines()]
    if len(paths) > 512:
        raise ValueError("Too many unit search paths")
    _check(deadline)
    return list(dict.fromkeys(paths)), runtimes


def _lstat(path, deadline):
    """Only ENOENT is absence; inspect ancestors so dangling links are not hidden."""
    path = _absolute(str(path))
    current = Path(path.anchor)
    result = current.lstat()
    for part in path.parts[1:]:
        _check(deadline)
        current /= part
        try:
            result = current.lstat()
        except FileNotFoundError:
            return None
        if stat.S_ISLNK(result.st_mode) and current != path:
            current.resolve(strict=True)  # Dangling ancestors and loops are UNKNOWN.
    _check(deadline)
    return result


def _artifacts(paths, deadline):
    """Inspect readable load directories, links, masks and dependency artifacts."""
    for directory in paths:
        info = _lstat(directory, deadline)
        if info is None:
            continue
        if stat.S_ISLNK(info.st_mode):
            directory = directory.resolve(strict=True)
        if not stat.S_ISDIR(directory.stat().st_mode):
            raise ValueError("Unit search path is not a directory")
        with os.scandir(directory) as entries:
            for entry in entries:
                _check(deadline)
                path = directory / entry.name
                info = path.lstat()
                if entry.name == UNIT or entry.name.startswith(UNIT + "."):
                    return True
                if stat.S_ISLNK(info.st_mode):
                    target = os.readlink(path)
                    if Path(target).name == UNIT:
                        return True
                    # An unreadable or cyclic alias cannot be used as absence proof.
                    path.resolve(strict=True)
                elif stat.S_ISDIR(info.st_mode) and entry.name.endswith((".wants", ".requires", ".upholds")):
                    if _lstat(path / UNIT, deadline) is not None:
                        return True
    return False


def _read(path, deadline):
    """Read only bounded non-secret procfs evidence."""
    _check(deadline)
    with Path(path).open(encoding="utf-8") as stream:
        text = stream.read(262145)
    if len(text) > 262144:
        raise ValueError("Process evidence exceeds limit")
    _check(deadline)
    return text


def _cgroup(text, uid):
    """Reject container views and systemd ownership relevant to this service."""
    lowered = text.lower()
    if any(marker in lowered for marker in ("docker", "kubepods", "libpod", "lxc", "machine.slice")):
        raise ValueError("Container process visibility is uncertain")
    if UNIT in text or f"user@{uid}.service" in text:
        raise ValueError("Cgroup evidence indicates user-service management")


def _kernel_thread(pid, deadline):
    """Require a readable, matching procfs flags record; names and empty argv are insufficient."""
    record = _read(f"/proc/{pid}/stat", deadline)
    head, closing, tail = record.rpartition(")")
    recorded_pid, opening, _comm = head.partition(" (")
    fields = tail.split()  # State is field 3; flags is field 9.
    if (not opening or not closing or recorded_pid != str(pid) or not tail.startswith(" ")
            or len(fields) < 7 or fields[0] not in {"R", "S", "D", "Z", "T", "t", "W", "X", "x", "K", "P", "I"}
            or not fields[6].isascii() or not fields[6].isdecimal() or int(fields[6]) > 0xFFFFFFFF):
        raise ValueError("Kernel-thread flags are unconfirmed")
    return bool(int(fields[6]) & PF_KTHREAD)


def _offline_manager_absent(runtimes, env, uid, deadline):
    """Require complete local process/namespace/socket/cgroup evidence, never a bus error."""
    if env.get("container") or env.get("SYSTEMD_NSPAWN_CONTAINER"):
        raise ValueError("Container environment is uncertain")
    for marker in ("/.dockerenv", "/run/.containerenv", "/run/systemd/container"):
        if _lstat(Path(marker), deadline) is not None:
            raise ValueError("Container process visibility is uncertain")
    mounts = _read("/proc/mounts", deadline)
    proc = [line.split() for line in mounts.splitlines() if len(line.split()) >= 4 and line.split()[1] == "/proc"]
    if len(proc) != 1 or proc[0][2] != "proc" or any(
            option.startswith("hidepid=") and option != "hidepid=0" for option in proc[0][3].split(",")):
        raise ValueError("Incomplete procfs visibility")
    namespaces = [Path(f"/proc/{pid}/ns/pid").stat() for pid in ("self", "1")]
    if (namespaces[0].st_dev, namespaces[0].st_ino) != (namespaces[1].st_dev, namespaces[1].st_ino):
        raise ValueError("PID namespace mismatch")
    status = _read("/proc/self/status", deadline)
    nspid = [line.split()[1:] for line in status.splitlines() if line.startswith("NSpid:")]
    if len(nspid) != 1 or len(nspid[0]) != 1:
        raise ValueError("Nested or unknown PID namespace")
    if _read("/proc/self/uid_map", deadline).split() != ["0", "0", "4294967295"]:
        raise ValueError("User namespace is uncertain")
    _cgroup(_read("/proc/self/cgroup", deadline), uid)
    _cgroup(_read("/proc/1/cgroup", deadline), uid)
    for base in runtimes:
        info = _lstat(base, deadline)
        if info is not None and (stat.S_ISLNK(info.st_mode) or info.st_uid != uid or not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077):
            raise ValueError("Runtime ownership is uncertain")
        for relative in ("bus", "systemd/private"):
            if _lstat(base / relative, deadline) is not None:
                raise ValueError("Runtime manager socket or artifact exists")
    for pid in psutil.pids():
        _check(deadline)
        try:
            process = psutil.Process(pid)
            uids = process.uids()
            if uid not in (uids.real, uids.effective):
                continue
            if process.status() == psutil.STATUS_ZOMBIE:
                continue
            argv = process.cmdline()
            if not argv:
                if _kernel_thread(pid, deadline):
                    continue
                raise ValueError("An owned process is uninspectable")
            name = process.name()
            if (name == "systemd" or (argv and Path(argv[0]).name == "systemd")) and ("--user" in argv or pid != 1):
                raise ValueError("A live user manager cannot be excluded")
            _cgroup(_read(f"/proc/{pid}/cgroup", deadline), uid)
        except psutil.NoSuchProcess:
            continue  # Confirmed exit, unlike an inaccessible process.
    _check(deadline)


def probe_manager(root):
    """Return one current finding with reason within a shared five-second budget."""
    deadline = time.monotonic() + 5
    stage = "unit search paths"
    try:
        env = dict(os.environ)
        uid = os.getuid()
        env.setdefault("XDG_RUNTIME_DIR", f"/run/user/{uid}")
        paths, runtimes = _unit_paths(env, Path.home(), uid, deadline)
        stage = "unit artifacts"
        if _artifacts(paths, deadline):
            return {"manager": "systemd", "reason": "PBCoinData unit, alias or mask is installed"}
        _check(deadline)
        stage = "user manager query"
        try:
            result = subprocess.run(["systemctl", "--user", "show", UNIT, "--all", "-p", "LoadState", "-p", "ActiveState", "-p", "MainPID", "-p", "Job"],
                                    capture_output=True, text=True, env=env,
                                    timeout=min(3, max(0.001, deadline - time.monotonic())))
            text = result.stdout.strip()
            _check(deadline)
            if text:
                values = dict(line.split("=", 1) for line in text.splitlines())
                if not {"LoadState", "ActiveState", "MainPID", "Job"} <= values.keys():
                    raise ValueError("Incomplete user-manager unit state")
                idle = (values["ActiveState"] == "inactive" and values["MainPID"] == "0"
                        and values["Job"] in {"", "0", "0 /"})
                if values["LoadState"] == "not-found" and result.returncode in (0, 4) and idle:
                    return {"manager": "legacy", "queried_absent": True, "reason": "User manager confirms absent inactive unit without a job"}
                if result.returncode == 0 and (values["LoadState"] in {"loaded", "masked", "error", "bad-setting", "stub", "merged"} or not idle):
                    return {"manager": "systemd", "reason": "User manager reports a managed unit or transition"}
                raise ValueError("Contradictory user-manager unit state")
        except (OSError, subprocess.TimeoutExpired):
            _check(deadline)
        stage = "offline process/runtime evidence"
        _offline_manager_absent(runtimes, env, uid, deadline)
        return {"manager": "legacy", "reason": "Complete offline unit and user-manager absence confirmed"}
    except Exception as exc:
        detail = str(exc) if isinstance(exc, (ValueError, TimeoutError)) else type(exc).__name__
        return {"manager": "unknown", "reason": f"Manager evidence incomplete ({stage}): {detail}"}
