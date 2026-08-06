"""Safe GitHub repository resolution for configured PBGui archives."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit

from pbgui_purefunc import PBGDIR, load_ini_snapshot


_PART_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
_SCP_RE = re.compile(r"^git@github\.com:([^/]+)/([^/]+)$")


@dataclass(frozen=True)
class GitHubRepository:
    """Canonical GitHub repository coordinates."""

    owner: str
    repository: str

    @property
    def slug(self) -> str:
        """Return the owner/repository slug."""
        return f"{self.owner}/{self.repository}"

    @property
    def browser_url(self) -> str:
        """Return the canonical public browser URL."""
        return f"https://github.com/{self.slug}"


def _validated_parts(owner: str, repository: str) -> GitHubRepository:
    repo = repository[:-4] if repository.lower().endswith(".git") else repository
    if not _PART_RE.fullmatch(owner) or not _PART_RE.fullmatch(repo) or owner in {".", ".."} or repo in {".", ".."}:
        raise ValueError("Invalid GitHub repository origin")
    return GitHubRepository(owner=owner, repository=repo)


def parse_github_repository_origin(remote_url: str) -> GitHubRepository:
    """Parse a credential-free GitHub HTTPS or SSH origin."""
    value = str(remote_url or "").strip()
    if not value or any(ord(ch) < 32 for ch in value):
        raise ValueError("Invalid GitHub repository origin")
    scp_match = _SCP_RE.fullmatch(value)
    if scp_match:
        return _validated_parts(scp_match.group(1), scp_match.group(2))

    parsed = urlsplit(value)
    if parsed.query or parsed.fragment or parsed.username not in {None, "git"} or parsed.password is not None:
        raise ValueError("Invalid GitHub repository origin")
    if parsed.hostname != "github.com" or parsed.port is not None:
        raise ValueError("Invalid GitHub repository origin")
    if parsed.scheme == "https" and parsed.username is not None:
        raise ValueError("Invalid GitHub repository origin")
    if parsed.scheme == "ssh" and parsed.username != "git":
        raise ValueError("Invalid GitHub repository origin")
    if parsed.scheme not in {"https", "ssh"}:
        raise ValueError("Invalid GitHub repository origin")
    decoded_path = unquote(parsed.path)
    if decoded_path != parsed.path or "\\" in decoded_path:
        raise ValueError("Invalid GitHub repository origin")
    parts = [part for part in decoded_path.split("/") if part]
    if len(parts) != 2:
        raise ValueError("Invalid GitHub repository origin")
    return _validated_parts(parts[0], parts[1])


def _validate_archive_name(name: str) -> str:
    value = str(name or "").strip()
    if not value or value in {".", ".."} or any(ch in value for ch in ("/", "\\", "\x00")):
        raise ValueError("Invalid archive name")
    if any(ord(ch) < 32 for ch in value):
        raise ValueError("Invalid archive name")
    return value


def archives_root() -> Path:
    """Return the configured local archive registry root."""
    return Path(PBGDIR) / "data" / "archives"


def _remote_url(archive_dir: Path, *, push: bool = False) -> str:
    argv = ["git", "-C", str(archive_dir), "remote", "get-url"]
    if push:
        argv.append("--push")
    argv.append("origin")
    completed = subprocess.run(
        argv,
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )
    if completed.returncode != 0:
        raise ValueError("Archive has no readable origin")
    return completed.stdout.strip()


def resolve_archive_repository(name: str, *, for_publish: bool = False) -> GitHubRepository:
    """Resolve one configured archive to a canonical GitHub repository."""
    archive_name = _validate_archive_name(name)
    archive_dir = archives_root() / archive_name
    if archive_dir.is_symlink() or not archive_dir.is_dir() or not (archive_dir / ".git").is_dir():
        raise ValueError("Archive is not available")
    fetch_repo = parse_github_repository_origin(_remote_url(archive_dir))
    if for_publish:
        push_repo = parse_github_repository_origin(_remote_url(archive_dir, push=True))
        if push_repo != fetch_repo:
            raise ValueError("Archive fetch and push origins differ")
    return fetch_repo


def archive_access_token() -> str:
    """Load the own-archive token server-side without exposing it to callers."""
    snapshot = load_ini_snapshot()
    if not snapshot.has_option("config_archive", "my_archive_access_token"):
        return ""
    return str(snapshot.get("config_archive", "my_archive_access_token") or "").strip()


def own_archive_name() -> str:
    """Return the configured writable archive name."""
    snapshot = load_ini_snapshot()
    if not snapshot.has_option("config_archive", "my_archive"):
        return ""
    return str(snapshot.get("config_archive", "my_archive") or "").strip()


def list_github_archives() -> list[dict[str, object]]:
    """List only safe GitHub archives without local paths, raw origins, or tokens."""
    root = archives_root()
    if not root.is_dir() or root.is_symlink():
        return []
    own = own_archive_name()
    token_configured = bool(archive_access_token())
    rows: list[dict[str, object]] = []
    for archive_dir in sorted(root.iterdir(), key=lambda path: path.name.lower()):
        if not archive_dir.is_dir() or archive_dir.is_symlink():
            continue
        try:
            repository = resolve_archive_repository(archive_dir.name)
        except ValueError:
            continue
        is_own = archive_dir.name == own
        can_publish = False
        if is_own and token_configured:
            try:
                resolve_archive_repository(archive_dir.name, for_publish=True)
                can_publish = True
            except ValueError:
                pass
        rows.append(
            {
                "name": archive_dir.name,
                "repository": repository.slug,
                "is_own": is_own,
                "can_publish": can_publish,
                "can_reference": True,
            }
        )
    return rows


def publish_release_asset(*, archive_name: str, asset_path: Path) -> dict[str, object]:
    """Create/update the fixed checksum release asset using server-side auth."""
    if _validate_archive_name(archive_name) != own_archive_name():
        raise ValueError("Publishing is only allowed to the configured own archive")
    repository = resolve_archive_repository(archive_name, for_publish=True)
    token = archive_access_token()
    if not token:
        raise RuntimeError("The own archive has no configured GitHub access token")
    gh = shutil.which("gh")
    if not gh:
        raise RuntimeError("GitHub CLI is not installed")
    asset = Path(asset_path)
    if asset.is_symlink() or not asset.is_file() or asset.name != "checksums.sqlite.gz":
        raise ValueError("Invalid checksum release asset")
    env = {
        **os.environ,
        "GH_TOKEN": token,
        "GH_PROMPT_DISABLED": "1",
        "GH_NO_UPDATE_NOTIFIER": "1",
    }

    def run(args: list[str], *, timeout: int = 180) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [gh, *args],
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
            env=env,
        )

    view = run(["release", "view", "checksums-latest", "--repo", repository.slug])
    if view.returncode != 0:
        created = run(
            [
                "release",
                "create",
                "checksums-latest",
                "--repo",
                repository.slug,
                "--title",
                "Latest OHLCV checksums",
                "--notes",
                "Daily PBGui OHLCV checksum reference.",
                "--latest=false",
            ]
        )
        if created.returncode != 0:
            raise RuntimeError("Unable to create the checksum release")
    uploaded = run(
        [
            "release",
            "upload",
            "checksums-latest",
            f"{asset}#checksums.sqlite.gz",
            "--clobber",
            "--repo",
            repository.slug,
        ],
        timeout=300,
    )
    if uploaded.returncode != 0:
        raise RuntimeError("Unable to upload the checksum release asset")
    return {"archive": archive_name, "repository": repository.slug, "asset": "checksums.sqlite.gz"}


def release_asset_url(archive_name: str) -> str:
    """Return the anonymous stable checksum asset URL for one archive."""
    repository = resolve_archive_repository(archive_name)
    return f"https://github.com/{repository.slug}/releases/download/checksums-latest/checksums.sqlite.gz"
