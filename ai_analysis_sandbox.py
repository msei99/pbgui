"""Capability-free Landlock and seccomp runner for approved AI analysis."""

from __future__ import annotations

import ctypes
import ctypes.util
import errno
import os
from pathlib import Path
import runpy
import sys


SERVICE = "AIAnalysisSandbox"
_LANDLOCK_CREATE_RULESET = 444
_LANDLOCK_ADD_RULE = 445
_LANDLOCK_RESTRICT_SELF = 446
_LANDLOCK_CREATE_RULESET_VERSION = 1
_LANDLOCK_RULE_PATH_BENEATH = 1
_PR_SET_NO_NEW_PRIVS = 38

_FS_EXECUTE = 1 << 0
_FS_WRITE_FILE = 1 << 1
_FS_READ_FILE = 1 << 2
_FS_READ_DIR = 1 << 3
_FS_REFER = 1 << 13
_FS_TRUNCATE = 1 << 14
_FS_IOCTL_DEV = 1 << 15
_FS_ABI_ONE = (1 << 13) - 1
_READ_EXECUTE = _FS_EXECUTE | _FS_READ_FILE | _FS_READ_DIR

_SECCOMP_ALLOW = 0x7FFF0000
_SECCOMP_ERRNO = 0x00050000
_DENIED_SYSCALLS = (
    "socket", "socketpair", "connect", "accept", "accept4", "bind", "listen",
    "sendto", "recvfrom", "sendmsg", "recvmsg", "recvmmsg", "sendmmsg",
    "getsockname", "getpeername", "setsockopt", "getsockopt", "shutdown",
    "io_uring_setup", "io_uring_enter", "io_uring_register", "ptrace",
    "process_vm_readv", "process_vm_writev", "pidfd_open", "pidfd_getfd",
    "pidfd_send_signal", "kill", "tkill", "tgkill", "mount", "umount2",
    "pivot_root", "chroot", "setns", "unshare", "open_by_handle_at", "bpf",
    "perf_event_open", "keyctl",
)


class _RulesetAttr(ctypes.Structure):
    """Landlock ABI-compatible ruleset prefix."""

    _fields_ = [("handled_access_fs", ctypes.c_uint64)]


class _PathBeneathAttr(ctypes.Structure):
    """Landlock path-beneath rule attributes."""

    _fields_ = [("allowed_access", ctypes.c_uint64), ("parent_fd", ctypes.c_int)]


def _landlock_restrict(paths: list[Path]) -> None:
    """Allow read/execute only below fixed runtime paths and the approved script."""

    libc = ctypes.CDLL(None, use_errno=True)
    libc.syscall.restype = ctypes.c_long
    abi = int(libc.syscall(_LANDLOCK_CREATE_RULESET, None, 0, _LANDLOCK_CREATE_RULESET_VERSION))
    if abi < 1:
        raise RuntimeError("Landlock filesystem sandbox is unavailable")
    handled = _FS_ABI_ONE
    if abi >= 2:
        handled |= _FS_REFER
    if abi >= 3:
        handled |= _FS_TRUNCATE
    if abi >= 5:
        handled |= _FS_IOCTL_DEV
    ruleset_attr = _RulesetAttr(handled_access_fs=handled)
    ruleset_fd = int(libc.syscall(
        _LANDLOCK_CREATE_RULESET,
        ctypes.byref(ruleset_attr),
        ctypes.sizeof(ruleset_attr),
        0,
    ))
    if ruleset_fd < 0:
        raise RuntimeError("Landlock filesystem sandbox could not be created")
    try:
        for path in paths:
            if not path.exists():
                continue
            path_fd = os.open(path, os.O_PATH | os.O_CLOEXEC)
            try:
                rule = _PathBeneathAttr(
                    allowed_access=(_READ_EXECUTE if path.is_dir() else _FS_READ_FILE) & handled,
                    parent_fd=path_fd,
                )
                if libc.syscall(_LANDLOCK_ADD_RULE, ruleset_fd, _LANDLOCK_RULE_PATH_BENEATH, ctypes.byref(rule), 0) != 0:
                    raise RuntimeError("Landlock filesystem rule could not be installed")
            finally:
                os.close(path_fd)
        if libc.prctl(_PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0:
            raise RuntimeError("Sandbox privilege restriction failed")
        if libc.syscall(_LANDLOCK_RESTRICT_SELF, ruleset_fd, 0) != 0:
            raise RuntimeError("Landlock filesystem sandbox could not be enforced")
    finally:
        os.close(ruleset_fd)


def _seccomp_restrict() -> None:
    """Deny network, cross-process, namespace, mount, and kernel attack syscalls."""

    library_name = ctypes.util.find_library("seccomp")
    if not library_name:
        raise RuntimeError("Seccomp sandbox is unavailable")
    library = ctypes.CDLL(library_name, use_errno=True)
    library.seccomp_init.argtypes = [ctypes.c_uint32]
    library.seccomp_init.restype = ctypes.c_void_p
    library.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    library.seccomp_syscall_resolve_name.restype = ctypes.c_int
    library.seccomp_rule_add.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int, ctypes.c_uint]
    library.seccomp_rule_add.restype = ctypes.c_int
    library.seccomp_load.argtypes = [ctypes.c_void_p]
    library.seccomp_load.restype = ctypes.c_int
    library.seccomp_release.argtypes = [ctypes.c_void_p]
    context = library.seccomp_init(_SECCOMP_ALLOW)
    if not context:
        raise RuntimeError("Seccomp sandbox could not be created")
    try:
        action = _SECCOMP_ERRNO | errno.EPERM
        for name in _DENIED_SYSCALLS:
            syscall = library.seccomp_syscall_resolve_name(name.encode("ascii"))
            if syscall < 0 or library.seccomp_rule_add(context, action, syscall, 0) != 0:
                raise RuntimeError("Seccomp sandbox rule could not be installed")
        if library.seccomp_load(context) != 0:
            raise RuntimeError("Seccomp sandbox could not be enforced")
    finally:
        library.seccomp_release(context)


def main() -> int:
    """Apply irreversible restrictions and execute the approved analysis script."""

    if len(sys.argv) != 3:
        raise RuntimeError("Sandbox arguments are invalid")
    script = Path(sys.argv[1]).resolve(strict=True)
    runtime_root = Path(sys.argv[2]).resolve(strict=True)
    allowed = [Path("/usr"), Path("/lib"), Path("/lib64"), runtime_root, script]
    os.chdir("/")
    _seccomp_restrict()
    _landlock_restrict(allowed)
    sys.argv = [str(script)]
    runpy.run_path(str(script), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
