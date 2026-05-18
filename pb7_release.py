from __future__ import annotations

import re
import subprocess
from pathlib import Path


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


def get_current_pb7_status(repo_dir: str | Path | None) -> tuple[str, str]:
    if not repo_dir:
        return "unknown", ""
    root = Path(repo_dir)
    current_commit = _run_git(["rev-parse", "HEAD"], root)
    current_branch = _run_git(["symbolic-ref", "--short", "HEAD"], root) or "unknown"
    return current_branch, current_commit


def read_local_pb7_version(repo_dir: str | Path | None) -> str:
    if not repo_dir:
        return "N/A"
    version_file = Path(repo_dir) / "src" / "passivbot_version.py"
    if not version_file.exists():
        return "N/A"
    try:
        content = version_file.read_text(encoding="utf-8")
    except Exception:
        return "N/A"
    match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
    if not match:
        return "N/A"
    return f"v{match.group(1)}"


def read_origin_pb7_version(repo_dir: str | Path | None, ref: str = "origin/master") -> str:
    if not repo_dir:
        return "N/A"
    root = Path(repo_dir)
    _run_git(["fetch", "origin"], root)
    version_text = _run_git(["show", f"{ref}:src/passivbot_version.py"], root)
    if not version_text:
        return "N/A"
    match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', version_text)
    if not match:
        return "N/A"
    return f"v{match.group(1)}"


def load_pb7_origin_commit(repo_dir: str | Path | None, ref: str = "origin/master") -> str:
    if not repo_dir:
        return ""
    root = Path(repo_dir)
    _run_git(["fetch", "origin"], root)
    return _run_git(["log", "-n", "1", "--pretty=format:%H", ref], root)


def load_pb7_branch_history(repo_dir: str | Path | None, limit: int = 50) -> dict[str, list[dict[str, str]]]:
    if not repo_dir:
        return {}
    root = Path(repo_dir)
    git_dir = root / ".git"
    if not git_dir.exists():
        return {}
    _run_git(["fetch", "origin"], root)
    branches_result = _run_subprocess(["git", "--git-dir", str(git_dir), "branch", "-a"], timeout=15, suppress_stderr=True)
    if not branches_result or branches_result.returncode != 0:
        return {}

    branches_data: dict[str, dict[str, object]] = {}
    remote_branches: set[str] = set()
    for line in str(branches_result.stdout or "").splitlines():
        branch_raw = line.strip().lstrip("* ")
        if branch_raw.startswith("remotes/origin/") and "HEAD ->" not in branch_raw:
            remote_branches.add(branch_raw.replace("remotes/origin/", ""))

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
        skip_patterns = ["v6.", "v5.", "v4.", "v3.", "v2.", "v1.", "v0.", "release/v6", "release/v1", "release/v0"]
        if any(branch_name.startswith(pattern) for pattern in skip_patterns):
            continue
        include_always = branch_name == "master" or branch_name.startswith("v7.") or branch_name.startswith("v7-")
        commits_result = _run_subprocess(
            ["git", "--git-dir", str(git_dir), "log", branch_ref, "-n", str(limit), "--pretty=format:%h|%H|%an|%ar|%at|%B%x00"],
            timeout=20,
            suppress_stderr=True,
        )
        if not commits_result or commits_result.returncode != 0:
            continue
        commits, latest_commit_timestamp = _parse_git_log_output(str(commits_result.stdout or ""))
        v7_release_timestamp = 1686000000
        if not include_always and (not latest_commit_timestamp or latest_commit_timestamp < v7_release_timestamp):
            continue
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


def load_more_pb7_commits(branch_name: str, repo_dir: str | Path | None, limit: int = 50) -> list[dict[str, str]]:
    if not repo_dir or not branch_name:
        return []
    root = Path(repo_dir)
    git_dir = root / ".git"
    if not git_dir.exists():
        return []
    _run_git(["fetch", "origin"], root)
    current_branch, _ = get_current_pb7_status(root)
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


def build_local_pb7_release_info(repo_dir: str | Path | None) -> dict[str, object]:
    current_branch, current_commit = get_current_pb7_status(repo_dir)
    return {
        "version": read_local_pb7_version(repo_dir),
        "current_branch": current_branch or "unknown",
        "current_commit": current_commit or "",
        "origin_version": read_origin_pb7_version(repo_dir),
        "origin_commit": load_pb7_origin_commit(repo_dir),
        "branches": load_pb7_branch_history(repo_dir),
    }
