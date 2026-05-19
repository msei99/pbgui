from __future__ import annotations

import re
import subprocess
from pathlib import Path

from pbgui_purefunc import PBGDIR, PBGUI_VERSION


def _run_subprocess(command: list[str], *, timeout: int = 20, suppress_stderr: bool = True):
    kwargs = {
        "text": True,
        "timeout": timeout,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.DEVNULL if suppress_stderr else subprocess.PIPE,
    }
    try:
        return subprocess.run(command, **kwargs)
    except Exception:
        return None


def _parse_git_log_output(raw_output: str):
    commits = []
    latest_commit_timestamp = None
    for commit_block in str(raw_output or "").split("\x00"):
        commit_block = commit_block.strip()
        if not commit_block:
            continue
        lines = commit_block.split("\n", 1)
        parts = lines[0].split("|", 5) if lines else []
        if len(parts) != 6:
            continue
        full_message = parts[5]
        if len(lines) > 1:
            full_message = full_message + "\n" + lines[1]
        commit_data = {
            "short": parts[0],
            "full": parts[1],
            "author": parts[2],
            "date": parts[3],
            "timestamp": int(parts[4]),
            "message": full_message.strip(),
        }
        commits.append(commit_data)
        if latest_commit_timestamp is None:
            latest_commit_timestamp = commit_data["timestamp"]
    return commits, latest_commit_timestamp


def _run_git(args: list[str], repo_dir: Path) -> str:
    git_dir = repo_dir / ".git"
    if not git_dir.exists():
        return ""
    result = _run_subprocess(["git", "--git-dir", str(git_dir)] + list(args), timeout=20, suppress_stderr=True)
    if not result or result.returncode != 0:
        return ""
    return str(result.stdout or "").strip()


def _read_origin_version(repo_dir: Path, ref: str = "origin/main") -> str:
    version_text = _run_git(["show", f"{ref}:pbgui_purefunc.py"], repo_dir)
    if version_text:
        match = re.search(r'PBGUI_VERSION\s*=\s*["\']([^"\']+)["\']', version_text)
        if match:
            return str(match.group(1) or "").strip() or "N/A"
    return "N/A"


def _read_local_version(repo_dir: Path) -> str:
    version_file = repo_dir / "pbgui_purefunc.py"
    try:
        if version_file.exists():
            content = version_file.read_text(encoding="utf-8", errors="ignore")
            match = re.search(r'PBGUI_VERSION\s*=\s*["\']([^"\']+)["\']', content)
            if match:
                return str(match.group(1) or "").strip() or "N/A"
    except Exception:
        pass
    return PBGUI_VERSION


def get_current_pbgui_status(repo_dir: Path | None = None) -> tuple[str, str]:
    root = Path(repo_dir or PBGDIR)
    current_commit = _run_git(["rev-parse", "HEAD"], root)
    current_branch = _run_git(["symbolic-ref", "--short", "HEAD"], root) or "unknown"
    return current_branch, current_commit


def load_pbgui_origin_commit(repo_dir: Path | None = None) -> str:
    root = Path(repo_dir or PBGDIR)
    _run_git(["fetch", "origin"], root)
    return _run_git(["log", "-n", "1", "--pretty=format:%H", "origin/main"], root)


def load_pbgui_origin_version(repo_dir: Path | None = None) -> str:
    root = Path(repo_dir or PBGDIR)
    _run_git(["fetch", "origin"], root)
    return _read_origin_version(root)


def load_pbgui_branch_history(repo_dir: Path | None = None, limit: int = 50) -> dict[str, list[dict[str, str]]]:
    root = Path(repo_dir or PBGDIR)
    _run_git(["fetch", "origin"], root)
    git_dir = root / ".git"
    if not git_dir.exists():
        return {}

    branches_result = _run_subprocess(["git", "--git-dir", str(git_dir), "branch", "-a"], timeout=15, suppress_stderr=True)
    if not branches_result or branches_result.returncode != 0:
        return {}

    branches_data: dict[str, dict[str, object]] = {}
    remote_branches: set[str] = set()
    for line in str(branches_result.stdout or "").splitlines():
        branch_raw = line.strip().lstrip("* ")
        if branch_raw.startswith("remotes/origin/") and "HEAD ->" not in branch_raw:
            remote_branches.add(branch_raw.replace("remotes/origin/", ""))

    current_branch, _ = get_current_pbgui_status(root)
    for line in str(branches_result.stdout or "").splitlines():
        branch_raw = line.strip().lstrip("* ")
        if not branch_raw or "HEAD ->" in branch_raw:
            continue
        if branch_raw.startswith("remotes/origin/"):
            branch_ref = branch_raw
            branch_name = branch_raw.replace("remotes/origin/", "")
        else:
            branch_name = branch_raw
            if branch_name in remote_branches:
                continue
            branch_ref = branch_raw
        if branch_name in branches_data:
            continue
        commits_result = _run_subprocess(
            [
                "git", "--git-dir", str(git_dir), "log", branch_ref, "-n", str(limit),
                "--pretty=format:%h|%H|%an|%ar|%at|%B%x00",
            ],
            timeout=20,
            suppress_stderr=True,
        )
        if not commits_result or commits_result.returncode != 0:
            continue
        commits, latest_commit_timestamp = _parse_git_log_output(str(commits_result.stdout or ""))
        if commits:
            branches_data[branch_name] = {
                "commits": commits,
                "latest_timestamp": latest_commit_timestamp,
            }

    sorted_branches = dict(
        sorted(
            branches_data.items(),
            key=lambda item: item[1]["latest_timestamp"] if item[1]["latest_timestamp"] else 0,
            reverse=True,
        )
    )
    return {name: data["commits"] for name, data in sorted_branches.items()}


def load_more_pbgui_commits(branch_name: str, repo_dir: Path | None = None, limit: int = 50) -> list[dict[str, str]]:
    root = Path(repo_dir or PBGDIR)
    git_dir = root / ".git"
    if not git_dir.exists() or not branch_name:
        return []
    _run_git(["fetch", "origin"], root)
    current_branch, _ = get_current_pbgui_status(root)
    branch_ref = f"remotes/origin/{branch_name}" if branch_name != current_branch else branch_name
    commits_result = _run_subprocess(
        ["git", "--git-dir", str(git_dir), "log", branch_ref, "-n", str(limit), "--pretty=format:%h|%H|%an|%ar|%at|%B%x00"],
        timeout=20,
        suppress_stderr=True,
    )
    if not commits_result or commits_result.returncode != 0:
        return []
    commits, _ = _parse_git_log_output(str(commits_result.stdout or ""))
    return commits


def build_local_pbgui_release_info(repo_dir: Path | None = None) -> dict[str, object]:
    root = Path(repo_dir or PBGDIR)
    current_branch, current_commit = get_current_pbgui_status(root)
    origin_commit = load_pbgui_origin_commit(root)
    version_origin = load_pbgui_origin_version(root)
    branches = load_pbgui_branch_history(root)
    return {
        "version": _read_local_version(root),
        "current_branch": current_branch or "unknown",
        "current_commit": current_commit or "",
        "origin_commit": origin_commit or "",
        "origin_version": version_origin,
        "branches": branches,
    }


def read_local_pbgui_version(repo_dir: str | Path | None = None) -> str:
    return _read_local_version(Path(repo_dir or PBGDIR))
