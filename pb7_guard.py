"""Pinned PB7 revision and fail-closed Passivbot major-version checks."""

from __future__ import annotations

import argparse
import ast
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path


PB7_PIN_PATH = Path(__file__).with_name("pb7_ref.txt")
PB7_UPSTREAM_URL = "https://github.com/enarjord/passivbot.git"
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")


def load_pb7_pinned_commit(path: Path = PB7_PIN_PATH) -> str:
    """Load and validate the exact PB7 commit shipped with PBGui."""
    try:
        commit = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise RuntimeError(f"Could not read PB7 pin file: {path}") from exc
    if not _COMMIT_RE.fullmatch(commit):
        raise RuntimeError(f"PB7 pin must be one lowercase 40-character commit SHA: {path}")
    return commit


PB7_PINNED_COMMIT = load_pb7_pinned_commit()


def _version_source(repo_dir: Path, ref: str | None = None) -> str:
    """Read Passivbot's version module from a checkout or a Git ref."""
    if ref:
        proc = subprocess.run(
            ["git", "-C", str(repo_dir), "show", f"{ref}:src/passivbot_version.py"],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "Git ref is unavailable").strip()
            raise RuntimeError(f"Could not inspect Passivbot ref {ref}: {detail}")
        return proc.stdout

    version_path = repo_dir / "src" / "passivbot_version.py"
    try:
        return version_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Could not read Passivbot version file: {version_path}") from exc


def _declared_version(source: str) -> str:
    """Return one top-level string assignment to ``__version__``."""
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise RuntimeError("Passivbot version file is not valid Python.") from exc
    versions: list[str] = []
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name) and target.id == "__version__":
            if not isinstance(node.value, ast.Constant) or not isinstance(node.value.value, str):
                raise RuntimeError("Passivbot __version__ must be a string literal.")
            versions.append(node.value.value)
    if len(versions) != 1:
        raise RuntimeError("Passivbot version file must declare __version__ exactly once.")
    return versions[0]


def _run_git(repo_dir: Path, args: list[str]) -> str:
    """Run a bounded Git command and return stdout or raise a concise error."""
    proc = subprocess.run(
        ["git", "-C", str(repo_dir), *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "Git command failed").strip()
        raise RuntimeError(detail)
    return proc.stdout.strip()


def fetch_passivbot_ref(
    repo_dir: str | Path,
    repo_url: str,
    *,
    branch: str = "master",
    ref: str | None = None,
) -> None:
    """Fetch full official history into an isolated ref without changing origin."""
    root = Path(repo_dir)
    is_shallow = _run_git(root, ["rev-parse", "--is-shallow-repository"]) == "true"
    args = ["fetch"]
    if is_shallow:
        args.append("--unshallow")
    args.extend([repo_url, f"{branch}:refs/remotes/pbgui-pb7-pin/{branch}"])
    _run_git(root, args)
    if ref:
        try:
            _run_git(root, ["cat-file", "-e", f"{ref}^{{commit}}"])
        except RuntimeError:
            _run_git(root, ["fetch", repo_url, f"{ref}:refs/pbgui/pb7-pin"])


def checkout_passivbot_ref(repo_dir: str | Path, ref: str) -> bool:
    """Force a detached checkout and report whether the commit changed."""
    root = Path(repo_dir)
    before = _run_git(root, ["rev-parse", "HEAD"])
    was_dirty = bool(_run_git(root, ["status", "--porcelain", "--untracked-files=no"]))
    _run_git(root, ["checkout", "--detach", "--force", ref])
    after = _run_git(root, ["rev-parse", "HEAD"])
    return before != after or was_dirty


def passivbot_ref_differs(repo_dir: str | Path, ref: str) -> bool:
    """Return whether a ref resolves to a commit other than current HEAD."""
    root = Path(repo_dir)
    current = _run_git(root, ["rev-parse", "HEAD"])
    target = _run_git(root, ["rev-parse", f"{ref}^{{commit}}"])
    dirty = bool(_run_git(root, ["status", "--porcelain", "--untracked-files=no"]))
    return current != target or dirty


def _process_start_time(process_dir: Path) -> str | None:
    """Read Linux process start time so a reused PID is never signalled."""
    try:
        suffix = (process_dir / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1]
        fields = suffix.split()
        return fields[19] if len(fields) > 19 else None
    except (OSError, IndexError):
        return None


def stop_passivbot_processes(
    repo_dir: str | Path,
    venv_dir: str | Path,
    *,
    proc_root: Path = Path("/proc"),
    timeout: float = 5.0,
) -> list[int]:
    """Stop processes launched from one Passivbot checkout or virtualenv."""
    repo_path = Path(repo_dir)
    venv_path = Path(venv_dir)
    repo_markers = {
        str(repo_path.absolute()) + "/src/",
        str(repo_path.resolve()) + "/src/",
    }
    venv_markers = {
        str(venv_path.absolute()) + "/bin/python",
        str(venv_path.resolve()) + "/bin/python",
    }
    matched: dict[int, str] = {}
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid == os.getpid():
            continue
        try:
            args = [arg.decode(errors="replace") for arg in (entry / "cmdline").read_bytes().split(b"\0") if arg]
        except OSError:
            continue
        if not args:
            continue
        executable_name = Path(args[0]).name.lower()
        executable_paths = {args[0]}
        if Path(args[0]).is_absolute():
            executable_paths.add(str(Path(args[0]).resolve()))
        from_venv = any(
            executable.startswith(marker)
            for executable in executable_paths
            for marker in venv_markers
        )
        runs_repo_script = executable_name.startswith("python") and any(
            any(candidate.startswith(marker) for marker in repo_markers)
            for arg in args[1:]
            if arg.endswith(".py")
            for candidate in ({arg, str(Path(arg).resolve())} if Path(arg).is_absolute() else {arg})
        )
        start_time = _process_start_time(entry)
        if start_time is not None and (from_venv or runs_repo_script):
            matched[pid] = start_time

    def same_process(pid: int) -> bool:
        return _process_start_time(proc_root / str(pid)) == matched[pid]

    for pid in sorted(matched):
        if not same_process(pid):
            continue
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + timeout
    remaining = set(matched)
    while remaining and time.monotonic() < deadline:
        remaining = {pid for pid in remaining if same_process(pid)}
        if remaining:
            time.sleep(0.1)
    for pid in remaining:
        if not same_process(pid):
            continue
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    return sorted(matched)


def verify_passivbot_major(repo_dir: str | Path, expected_major: int, *, ref: str | None = None) -> str:
    """Return the Passivbot version or fail when it is not the expected major."""
    source = _version_source(Path(repo_dir), ref=ref)
    version = _declared_version(source)
    major = version.split(".", 1)[0]
    if major != str(expected_major):
        target = f"ref {ref}" if ref else "checkout"
        raise RuntimeError(
            f"Refusing {target}: expected Passivbot v{expected_major}, found v{version}."
        )
    return version


def main() -> int:
    """Validate a Passivbot checkout or Git ref from installer playbooks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--expected-major", required=True, type=int)
    parser.add_argument("--ref", default="")
    parser.add_argument("--fetch-url", default="")
    parser.add_argument("--fetch-branch", default="master")
    parser.add_argument("--checkout", action="store_true")
    parser.add_argument("--stop-processes", action="store_true")
    parser.add_argument("--venv", type=Path)
    args = parser.parse_args()
    try:
        if args.fetch_url:
            fetch_passivbot_ref(
                args.repo,
                args.fetch_url,
                branch=args.fetch_branch,
                ref=args.ref.strip() or None,
            )
        version = verify_passivbot_major(
            args.repo,
            args.expected_major,
            ref=args.ref.strip() or None,
        )
        checkout_required = bool(args.ref.strip()) and passivbot_ref_differs(args.repo, args.ref.strip())
        changed = False
        stopped: list[int] = []
        if args.stop_processes:
            if args.venv is None:
                raise RuntimeError("--stop-processes requires --venv.")
            stopped = stop_passivbot_processes(args.repo, args.venv)
        if args.checkout:
            if not args.ref.strip():
                raise RuntimeError("--checkout requires --ref.")
            changed = checkout_passivbot_ref(args.repo, args.ref.strip())
            verify_passivbot_major(args.repo, args.expected_major)
    except RuntimeError as exc:
        print(f"PB7 guard failed: {exc}", file=sys.stderr)
        return 1
    print(f"Passivbot v{version} verified.")
    if args.ref.strip():
        print(f"checkout_required={'true' if checkout_required else 'false'}")
    if args.checkout:
        print(f"checkout_changed={'true' if changed else 'false'}")
    if args.stop_processes:
        print(f"stopped_processes={len(stopped)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
