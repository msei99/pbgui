"""Helpers for PBGui config archive layout, migration, and listing."""

from __future__ import annotations

import datetime
import hashlib
import html
import json
import math
import os
import re
import secrets
import shutil
import stat
import subprocess
import tempfile
from contextlib import contextmanager
from itertools import islice
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import quote

from file_lock import advisory_file_lock
from secure_files import read_regular_file_nofollow


ARCHIVE_LAYOUT_ROOT = Path("pbgui") / "configs"
ARCHIVE_REPORT = Path("pbgui") / "archive_migration_report.json"
ARCHIVE_MANIFEST = Path("pbgui") / "archive_manifest.json"
ARCHIVE_README_CONFIG = Path("pbgui") / "readme_config.json"
ARCHIVE_README = Path("README.md")
ARCHIVE_SCORES = Path("SCORES.md")
ARCHIVE_SCORES_HTML = Path("SCORES.html")
README_SCORES_START = "<!-- pbgui:scores:start -->"
README_SCORES_END = "<!-- pbgui:scores:end -->"
README_SCORES_PLACEHOLDER = "_PBGui score overview has not been generated yet._"
ARCHIVE_SCORE_VERSION = 2
_SAFE_PART_RE = re.compile(r"[^A-Za-z0-9._-]+")
_SAFE_VERSION_RE = re.compile(r"[A-Za-z0-9._-]{1,120}\Z")

_SCORE_GROUP_WEIGHTS = {
    "returns": 0.25,
    "risk": 0.25,
    "ratios": 0.20,
    "curve": 0.15,
    "execution": 0.10,
    "data_quality": 0.05,
}

_SCORE_METRICS = [
    {"group": "returns", "label": "ADG", "lower": False, "keys": ["adg_w_usd", "adg_usd", "adg_w", "adg"]},
    {"group": "returns", "label": "MDG", "lower": False, "keys": ["mdg_w_usd", "mdg_usd", "mdg_w", "mdg"]},
    {"group": "returns", "label": "Gain", "lower": False, "keys": ["gain_usd", "gain"]},
    {"group": "returns", "label": "ADG / Exposure", "lower": False, "keys": ["adg_per_exposure_w_usd", "adg_per_exposure_usd", "adg_per_exposure_w", "adg_per_exposure"]},
    {"group": "returns", "label": "Gain / Exposure", "lower": False, "keys": ["gain_per_exposure_w_usd", "gain_per_exposure_usd", "gain_per_exposure_w", "gain_per_exposure"]},
    {"group": "risk", "label": "Worst Drawdown", "lower": True, "keys": ["drawdown_worst_w_usd", "drawdown_worst_usd", "drawdown_worst_w", "drawdown_worst"]},
    {"group": "risk", "label": "Expected Shortfall", "lower": True, "keys": ["expected_shortfall_1pct_w_usd", "expected_shortfall_1pct_usd", "expected_shortfall_5pct_w_usd", "expected_shortfall_5pct_usd"], "prefixes": ["expected_shortfall_"]},
    {"group": "risk", "label": "Equity/Balance Diff", "lower": True, "keys": ["equity_balance_diff_neg_max_usd", "equity_balance_diff_neg_max", "equity_balance_diff_max_usd", "equity_balance_diff_max"], "prefixes": ["equity_balance_diff_"]},
    {"group": "risk", "label": "Trade Loss", "lower": True, "keys": ["trade_loss_max_usd", "trade_loss_mean_usd", "trade_loss_max", "trade_loss_mean"], "prefixes": ["trade_loss_"]},
    {"group": "risk", "label": "Loss / Profit", "lower": True, "keys": ["loss_profit_ratio", "paper_loss_ratio", "paper_loss_mean_ratio"]},
    {"group": "ratios", "label": "Sharpe", "lower": False, "keys": ["sharpe_ratio_w_usd", "sharpe_ratio_usd", "sharpe_ratio_w", "sharpe_ratio"]},
    {"group": "ratios", "label": "Sortino", "lower": False, "keys": ["sortino_ratio_w_usd", "sortino_ratio_usd", "sortino_ratio_w", "sortino_ratio"]},
    {"group": "ratios", "label": "Omega", "lower": False, "keys": ["omega_ratio_w_usd", "omega_ratio_usd", "omega_ratio_w", "omega_ratio"]},
    {"group": "ratios", "label": "Calmar", "lower": False, "keys": ["calmar_ratio_w_usd", "calmar_ratio_usd", "calmar_ratio_w", "calmar_ratio"]},
    {"group": "ratios", "label": "Sterling", "lower": False, "keys": ["sterling_ratio_w_usd", "sterling_ratio_usd", "sterling_ratio_w", "sterling_ratio"]},
    {"group": "ratios", "label": "Win Rate", "lower": False, "keys": ["win_rate"]},
    {"group": "curve", "label": "Choppiness", "lower": True, "keys": ["equity_choppiness_w_usd", "equity_choppiness_usd", "equity_choppiness_w", "equity_choppiness"]},
    {"group": "curve", "label": "Jerkiness", "lower": True, "keys": ["equity_jerkiness_w_usd", "equity_jerkiness_usd", "equity_jerkiness_w", "equity_jerkiness"]},
    {"group": "curve", "label": "Fit Error", "lower": True, "keys": ["exponential_fit_error_w_usd", "exponential_fit_error_usd", "exponential_fit_error_w", "exponential_fit_error"]},
    {"group": "execution", "label": "Positions / Day", "lower": False, "keys": ["positions_held_per_day"]},
    {"group": "execution", "label": "Position Hold Time", "lower": True, "keys": ["position_held_hours_mean", "position_held_days_mean"], "prefixes": ["position_held_hours_", "position_held_days_"]},
    {"group": "execution", "label": "Volume / Day", "lower": False, "keys": ["volume_pct_per_day_avg"]},
    {"group": "execution", "label": "Peak Recovery", "lower": True, "keys": ["peak_recovery_hours_max", "peak_recovery_days_max"], "prefixes": ["peak_recovery_hours_", "peak_recovery_days_"]},
    {"group": "execution", "label": "High Exposure", "lower": True, "keys": ["high_exposure_hours_max", "high_exposure_days_max"], "prefixes": ["high_exposure_hours_", "high_exposure_days_"]},
    {"group": "execution", "label": "Hard Stops", "lower": True, "keys": ["hard_stop_triggers_per_year", "hard_stop_restarts_per_year"], "prefixes": ["hard_stop_"]},
]


def safe_path_part(value: Any, default: str = "unknown") -> str:
    """Return a filesystem-safe single path segment."""
    text = str(value or "").strip()
    text = _SAFE_PART_RE.sub("_", text).strip("._-")
    if not text or text in {".", ".."}:
        text = default
    return text[:120]


def utc_now_iso() -> str:
    """Return a compact UTC ISO timestamp for archive metadata."""
    return datetime.datetime.now(datetime.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json_file(path: Path) -> dict:
    """Load a JSON object from disk, returning an empty object on parse/read errors."""
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _absolute_path(path: Path) -> Path:
    """Return an absolute normalized path without resolving symlinks."""
    return Path(os.path.abspath(Path(path).expanduser()))


@contextmanager
def archive_transaction(archive_root: Path) -> Iterator[None]:
    """Serialize archive mutations across threads and processes, reentrantly."""
    absolute_root = _absolute_path(archive_root)
    lock_id = hashlib.sha256(str(absolute_root).encode("utf-8")).hexdigest()
    lock_target = absolute_root.parent / ".pbgui-archive-locks" / lock_id
    with advisory_file_lock(lock_target):
        yield


def _reject_symlink_components(path: Path) -> None:
    """Reject any existing symlink component in an absolute path."""
    absolute = _absolute_path(path)
    for component in reversed([absolute, *absolute.parents]):
        if component.is_symlink():
            raise RuntimeError(f"Path contains a symlink component: {component}")


def _validate_archive_path(path: Path, archive_root: Path, *, require_exists: bool = False) -> Path:
    """Validate lexical/resolved containment and reject symlink path components."""
    absolute_root = _absolute_path(archive_root)
    absolute_path = _absolute_path(path)
    try:
        relative = absolute_path.relative_to(absolute_root)
    except ValueError as exc:
        raise RuntimeError(f"Archive path escaped root: {path}") from exc
    if absolute_root.is_symlink():
        raise RuntimeError(f"Archive root must not be a symlink: {archive_root}")
    if absolute_root.exists() and not absolute_root.is_dir():
        raise RuntimeError(f"Archive root is not a directory: {archive_root}")
    current = absolute_root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise RuntimeError(f"Archive path contains a symlink: {current}")
    try:
        absolute_path.resolve(strict=False).relative_to(absolute_root.resolve(strict=False))
    except ValueError as exc:
        raise RuntimeError(f"Resolved archive path escaped root: {path}") from exc
    if require_exists and not absolute_path.exists():
        raise RuntimeError(f"Archive path does not exist: {path}")
    return absolute_path


def _ensure_archive_directory(path: Path, archive_root: Path) -> Path:
    """Create an archive directory tree after validating every destination component."""
    absolute_root = _absolute_path(archive_root)
    absolute_path = _validate_archive_path(path, archive_root)
    if not absolute_root.exists():
        if absolute_root.is_symlink():
            raise RuntimeError(f"Archive root must not be a symlink: {archive_root}")
        absolute_root.mkdir()
    current = absolute_root
    relative = absolute_path.relative_to(absolute_root)
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise RuntimeError(f"Archive destination contains a symlink: {current}")
        current.mkdir(exist_ok=True)
        if not current.is_dir():
            raise RuntimeError(f"Archive destination is not a directory: {current}")
    return absolute_path


def _read_json_object_nofollow(path: Path, approved_root: Path, *, required: bool = False) -> dict | None:
    """Read a regular JSON object below a trusted root without following symlinks."""
    try:
        absolute_path = _validate_archive_path(path, approved_root)
    except RuntimeError:
        if required:
            raise
        return None
    if not absolute_path.exists():
        if required:
            raise RuntimeError(f"Required archive JSON is missing: {path}")
        return None
    try:
        raw = read_regular_file_nofollow(absolute_path, _absolute_path(approved_root))
        data = json.loads(raw.decode("utf-8"))
    except (OSError, RuntimeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        if required:
            raise RuntimeError(f"Invalid archive JSON object: {path}") from exc
        return None
    if not isinstance(data, dict):
        if required:
            raise RuntimeError(f"Archive JSON must contain an object: {path}")
        return None
    return data


@contextmanager
def _archive_directory_fd(path: Path, archive_root: Path, *, create: bool) -> Iterator[int]:
    """Open an archive directory through no-follow directory descriptors."""
    absolute_root = _absolute_path(archive_root)
    absolute_path = _validate_archive_path(path, absolute_root)
    _reject_symlink_components(absolute_root.parent)
    if not absolute_root.exists():
        if not create:
            raise RuntimeError(f"Archive root does not exist: {archive_root}")
        absolute_root.mkdir(mode=0o700)
    flags = os.O_RDONLY | os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    current_fd = os.open(absolute_root, flags)
    try:
        for part in absolute_path.relative_to(absolute_root).parts:
            try:
                next_fd = os.open(part, flags, dir_fd=current_fd)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(part, mode=0o700, dir_fd=current_fd)
                next_fd = os.open(part, flags, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
        yield current_fd
    finally:
        os.close(current_fd)


def _atomic_write_archive_bytes(path: Path, content: bytes, archive_root: Path) -> None:
    """Atomically write bytes below an archive root without following symlinks."""
    absolute_path = _validate_archive_path(path, archive_root)
    temp_name = ""
    with _archive_directory_fd(absolute_path.parent, archive_root, create=True) as parent_fd:
        try:
            destination_stat = os.stat(absolute_path.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            destination_stat = None
        if destination_stat is not None and stat.S_ISLNK(destination_stat.st_mode):
            raise RuntimeError(f"Archive destination must not be a symlink: {absolute_path}")

        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        temp_fd = -1
        for _ in range(100):
            temp_name = f".{absolute_path.name}.{secrets.token_hex(12)}.tmp"
            try:
                temp_fd = os.open(temp_name, flags, 0o600, dir_fd=parent_fd)
                break
            except FileExistsError:
                continue
        if temp_fd < 0:
            raise RuntimeError(f"Unable to allocate archive temporary file for: {absolute_path}")
        try:
            with os.fdopen(temp_fd, "wb") as handle:
                temp_fd = -1
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                destination_stat = os.stat(absolute_path.name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                destination_stat = None
            if destination_stat is not None and stat.S_ISLNK(destination_stat.st_mode):
                raise RuntimeError(f"Archive destination must not be a symlink: {absolute_path}")
            os.replace(temp_name, absolute_path.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            temp_name = ""
        finally:
            if temp_fd >= 0:
                os.close(temp_fd)
            if temp_name:
                try:
                    os.unlink(temp_name, dir_fd=parent_fd)
                except FileNotFoundError:
                    pass


def _atomic_write_archive_json(path: Path, payload: dict, archive_root: Path) -> None:
    """Atomically write archive JSON with a unique no-follow temporary file."""
    encoded = (json.dumps(payload, indent=4) + "\n").encode("utf-8")
    _atomic_write_archive_bytes(path, encoded, archive_root)


def write_archive_json(path: Path, payload: dict, archive_root: Path) -> None:
    """Write a JSON object below an archive root through the secure archive writer."""
    cleaned = dict(payload)
    cleaned.pop("_pbgui_param_status", None)
    with archive_transaction(archive_root):
        absolute_path = _validate_archive_path(path, archive_root)
        legacy_temp = absolute_path.with_suffix(absolute_path.suffix + ".tmp")
        if legacy_temp.is_symlink():
            raise RuntimeError(f"Archive temporary path must not be a symlink: {legacy_temp}")
        _atomic_write_archive_json(absolute_path, cleaned, archive_root)


def _atomic_write_archive_text(path: Path, content: str, archive_root: Path) -> None:
    """Atomically write archive text with a unique no-follow temporary file."""
    _atomic_write_archive_bytes(path, content.encode("utf-8"), archive_root)


def _read_archive_text_nofollow(path: Path, archive_root: Path) -> str:
    """Read optional archive text without following a destination symlink."""
    absolute_path = _validate_archive_path(path, archive_root)
    if not absolute_path.exists():
        return ""
    try:
        return read_regular_file_nofollow(absolute_path, _absolute_path(archive_root)).decode("utf-8")
    except (OSError, RuntimeError, UnicodeDecodeError) as exc:
        raise RuntimeError(f"Invalid archive text file: {path}") from exc


def _validate_directory_tree_no_symlinks(path: Path) -> Path:
    """Validate that an existing directory and all descendants contain no symlinks."""
    absolute = _absolute_path(path)
    _reject_symlink_components(absolute)
    if absolute.is_symlink() or not absolute.is_dir():
        raise RuntimeError(f"Source must be a regular directory, not a symlink: {path}")
    for root, dirs, files in os.walk(absolute, followlinks=False):
        for name in [*dirs, *files]:
            item = Path(root) / name
            if item.is_symlink():
                raise RuntimeError(f"Source tree contains a symlink: {item}")
    return absolute


def atomic_write_json(path: Path, payload: dict) -> None:
    """Write JSON atomically with PBGui's standard temp-file replacement pattern."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=4)
        handle.write("\n")
    os.replace(str(tmp), str(path))


def atomic_write_text(path: Path, content: str) -> None:
    """Write text atomically with PBGui's standard temp-file replacement pattern."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(content)
    os.replace(str(tmp), str(path))


def archive_manifest_path(archive_root: Path) -> Path:
    """Return the archive manifest path for an archive root."""
    return archive_root / ARCHIVE_MANIFEST


def load_archive_manifest(archive_root: Path) -> dict | None:
    """Load and minimally validate an archive manifest."""
    manifest = _read_json_object_nofollow(archive_manifest_path(archive_root), archive_root)
    if not manifest:
        return None
    if manifest.get("schema_version") != 1:
        return None
    if not isinstance(manifest.get("items"), list):
        return None
    if any(not isinstance(item, dict) for item in manifest["items"]):
        return None
    return manifest


def archive_readme_config_path(archive_root: Path) -> Path:
    """Return the per-archive README configuration path."""
    return archive_root / ARCHIVE_README_CONFIG


def archive_readme_path(archive_root: Path) -> Path:
    """Return the archive README path."""
    return archive_root / ARCHIVE_README


def archive_scores_path(archive_root: Path) -> Path:
    """Return the generated full score table path."""
    return archive_root / ARCHIVE_SCORES


def archive_scores_html_path(archive_root: Path) -> Path:
    """Return the generated interactive score table path."""
    return archive_root / ARCHIVE_SCORES_HTML


def archive_git_remote_url(archive_root: Path) -> str:
    """Return the archive origin remote URL, if configured."""
    if not (archive_root / ".git").exists():
        return ""
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=str(archive_root),
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return ""
    if result.returncode != 0:
        return ""
    return (result.stdout or "").strip()


def github_pages_base_url(remote_url: str) -> str:
    """Derive a GitHub Pages base URL from a GitHub remote URL."""
    raw = str(remote_url or "").strip()
    match = re.match(r"^(?:https?://github\.com/|git@github\.com:|ssh://git@github\.com/)([^/]+)/([^/]+?)(?:\.git)?/?$", raw)
    if not match:
        return ""
    owner = match.group(1)
    repo = match.group(2)
    if repo.lower() == f"{owner.lower()}.github.io":
        return f"https://{owner.lower()}.github.io/"
    return f"https://{owner.lower()}.github.io/{repo}/"


def github_repo_base_url(remote_url: str) -> str:
    """Derive a GitHub repository URL from a GitHub remote URL."""
    raw = str(remote_url or "").strip()
    match = re.match(r"^(?:https?://github\.com/|git@github\.com:|ssh://git@github\.com/)([^/]+)/([^/]+?)(?:\.git)?/?$", raw)
    if not match:
        return ""
    return f"https://github.com/{match.group(1)}/{match.group(2)}"


def archive_git_branch(archive_root: Path) -> str:
    """Return the current archive git branch, falling back to main."""
    if not (archive_root / ".git").exists():
        return "main"
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(archive_root),
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return "main"
    branch = (result.stdout or "").strip()
    if result.returncode != 0 or not branch or branch == "HEAD":
        return "main"
    return branch


def archive_github_tree_url(archive_root: Path, relative_path: str) -> str:
    """Return a GitHub tree URL for an archive path when derivable."""
    base = github_repo_base_url(archive_git_remote_url(archive_root))
    rel = str(relative_path or "").strip().strip("/")
    if not base or not rel:
        return ""
    branch = archive_git_branch(archive_root)
    return f"{base}/tree/{quote(branch, safe='')}/{quote(rel)}"


def archive_github_pages_url(archive_root: Path, target: Path = ARCHIVE_SCORES_HTML) -> str:
    """Return the GitHub Pages URL for an archive file when derivable."""
    base = github_pages_base_url(archive_git_remote_url(archive_root))
    if not base:
        return ""
    return base + quote(target.as_posix())


def normalize_archive_readme_config(config: dict | None, archive_name: str = "") -> dict:
    """Normalize per-archive README config values."""
    data = config if isinstance(config, dict) else {}
    title = str(data.get("title") or archive_name or "PBGui Config Archive").strip()
    if not title:
        title = archive_name or "PBGui Config Archive"
    static_markdown = str(data.get("static_markdown") or "").replace("\r\n", "\n").replace("\r", "\n")
    return {
        "schema_version": 1,
        "title": title[:160],
        "static_markdown": static_markdown[:50000],
    }


def load_archive_readme_config(archive_root: Path) -> dict:
    """Load the per-archive README config, returning defaults when absent."""
    config = _read_json_object_nofollow(archive_readme_config_path(archive_root), archive_root) or {}
    if config.get("schema_version") != 1:
        config = {}
    return normalize_archive_readme_config(config, archive_root.name)


def save_archive_readme_config(archive_root: Path, config: dict) -> dict:
    """Persist the per-archive README config and return the normalized payload."""
    with archive_transaction(archive_root):
        normalized = normalize_archive_readme_config(config, archive_root.name)
        _atomic_write_archive_json(archive_readme_config_path(archive_root), normalized, archive_root)
        return normalized


def _extract_readme_scores_block(existing: str, scores_markdown: str | None = None) -> str:
    """Return the generated score block body to keep or write."""
    if scores_markdown is not None:
        return str(scores_markdown).strip() or README_SCORES_PLACEHOLDER
    start = existing.find(README_SCORES_START)
    end = existing.find(README_SCORES_END)
    if start >= 0 and end > start:
        body = existing[start + len(README_SCORES_START):end].strip()
        return body or README_SCORES_PLACEHOLDER
    return README_SCORES_PLACEHOLDER


def build_archive_readme_content(
    archive_root: Path,
    config: dict | None = None,
    *,
    scores_markdown: str | None = None,
    existing_content: str = "",
) -> str:
    """Build README content from static archive config plus the generated score block."""
    normalized = normalize_archive_readme_config(config or load_archive_readme_config(archive_root), archive_root.name)
    static_markdown = normalized["static_markdown"].strip()
    scores_body = _extract_readme_scores_block(existing_content, scores_markdown)
    parts = [f"# {normalized['title']}", ""]
    if static_markdown:
        parts.extend([static_markdown, ""])
    parts.extend([README_SCORES_START, scores_body, README_SCORES_END, ""])
    return "\n".join(parts)


def update_archive_readme(
    archive_root: Path,
    config: dict | None = None,
    *,
    scores_markdown: str | None = None,
) -> str:
    """Write README.md while preserving/replacing only PBGui's generated score block."""
    with archive_transaction(archive_root):
        readme = archive_readme_path(archive_root)
        existing = _read_archive_text_nofollow(readme, archive_root)
        content = build_archive_readme_content(
            archive_root,
            config or load_archive_readme_config(archive_root),
            scores_markdown=scores_markdown,
            existing_content=existing,
        )
        _atomic_write_archive_text(readme, content, archive_root)
        return content


def json_fingerprint(data: Any) -> str:
    """Return a stable short fingerprint for JSON-serializable data."""
    encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:8]


def directory_fingerprint(path: Path) -> str:
    """Return a stable short fingerprint for a directory without following symlinks."""
    digest = hashlib.sha256()
    for item in sorted(path.rglob("*")):
        try:
            rel = item.relative_to(path).as_posix().encode("utf-8", errors="replace")
        except ValueError:
            continue
        digest.update(rel)
        if item.is_symlink():
            digest.update(b"symlink")
            try:
                digest.update(os.readlink(item).encode("utf-8", errors="replace"))
            except OSError:
                pass
            continue
        if not item.is_file():
            continue
        digest.update(b"file")
        try:
            with open(item, "rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
        except OSError:
            digest.update(b"unreadable")
    return digest.hexdigest()[:8]


def config_version_info(config: dict, *, fingerprint: str | None = None) -> dict:
    """Return normalized generation-neutral config version metadata for archive paths."""
    value = (config or {}).get("config_version")
    raw_version = value if isinstance(value, str) else ""
    has_version = bool(_SAFE_VERSION_RE.fullmatch(raw_version))
    version_segment = raw_version if has_version else "unknown"
    is_v8 = bool(re.match(r"^v8(?:[._-]|$)", raw_version, re.IGNORECASE))
    config_family = "pb8" if is_v8 else "pb7"
    generation = "v8" if is_v8 else "v7"
    return {
        "config_version": version_segment,
        "config_version_raw": raw_version,
        "has_config_version": has_version,
        "config_family": config_family,
        "backtest_version": generation,
        "optimize_version": generation,
        "pb7_config_version": version_segment,
        "pb7_config_version_raw": raw_version,
        "has_pb7_config_version": has_version,
        "fingerprint": fingerprint or json_fingerprint(config or {}),
        "pbgui_version": str(((config or {}).get("pbgui") or {}).get("version") or ""),
    }


def is_new_backtest_result_path(result_dir: Path, archive_root: Path) -> bool:
    """Return true when a result path already follows the versioned backtest layout."""
    try:
        rel = result_dir.resolve().relative_to(archive_root.resolve())
    except ValueError:
        return False
    parts = rel.parts
    return len(parts) >= 6 and parts[0] == "pbgui" and parts[1] == "configs" and parts[3] == "backtests"


def is_inside_archive(path: Path, archive_root: Path) -> bool:
    """Return true for a non-symlink path contained inside archive_root."""
    try:
        _validate_archive_path(path, archive_root)
        return True
    except (OSError, RuntimeError, ValueError):
        return False


def read_result_config(result_dir: Path) -> dict:
    """Load a backtest result's config.json if present."""
    return load_json_file(result_dir / "config.json")


def _config_name_from_result(result_dir: Path, config: dict, archive_root: Path | None = None) -> str:
    """Derive a stable config name for an archived backtest result."""
    backtest = (config or {}).get("backtest") or {}
    base_dir = str(backtest.get("base_dir") or "").strip()
    if base_dir:
        return safe_path_part(Path(base_dir).name, "config")
    if archive_root is not None:
        try:
            rel_parts = result_dir.resolve().relative_to(archive_root.resolve()).parts
            if len(rel_parts) >= 6 and rel_parts[0] == "pbgui" and rel_parts[1] == "configs" and rel_parts[3] == "backtests":
                return safe_path_part(rel_parts[4], "config")
            if len(rel_parts) >= 3:
                return safe_path_part(rel_parts[-3], "config")
        except ValueError:
            pass
    parts = result_dir.parts
    if len(parts) >= 3:
        return safe_path_part(parts[-3], "config")
    return safe_path_part(result_dir.parent.name, "config")


def _exchange_dir_from_config(config: dict) -> str:
    """Derive the archive exchange directory from backtest.exchanges."""
    backtest = (config or {}).get("backtest") or {}
    exchanges = backtest.get("exchanges")
    if isinstance(exchanges, list) and len(exchanges) == 1:
        return safe_path_part(exchanges[0], "combined").lower()
    if isinstance(exchanges, str) and exchanges.strip():
        return safe_path_part(exchanges, "combined").lower()
    return "combined"


def _normalized_coin_values(value: Any) -> list[str]:
    """Return a de-duplicated coin list from PB7 result metadata values."""
    raw_values: list[Any] = []
    if isinstance(value, dict):
        side_values: list[str] = []
        for side in ("long", "short"):
            if side in value:
                side_values.extend(_normalized_coin_values(value.get(side)))
        if side_values:
            return _normalized_coin_values(side_values)
        raw_values.extend(value.keys())
    elif isinstance(value, (list, tuple, set)):
        raw_values.extend(value)
    elif isinstance(value, str):
        raw_values.extend(part for part in re.split(r"[,\s]+", value) if part)
    else:
        return []

    seen: set[str] = set()
    coins: list[str] = []
    for raw in raw_values:
        text = str(raw or "").strip().upper()
        if not text or text in seen:
            continue
        seen.add(text)
        coins.append(text)
    return coins


def _coins_from_result_metadata(result_dir: Path, config: dict, archive_root: Path | None = None) -> list[str]:
    """Extract the visible coin list for an archived backtest result."""
    dataset = (
        _read_json_object_nofollow(result_dir / "dataset.json", archive_root) or {}
        if archive_root is not None
        else load_json_file(result_dir / "dataset.json")
    )
    for value in (
        dataset.get("coins"),
        dataset.get("approved_coins"),
        dataset.get("side_membership"),
        dataset.get("coin_index"),
        ((config or {}).get("live") or {}).get("approved_coins") if isinstance(config, dict) else None,
    ):
        coins = _normalized_coin_values(value)
        if coins:
            return coins
    return []


def derive_backtest_archive_relative_path(result_dir: Path, archive_root: Path) -> tuple[Path, dict]:
    """Derive the new versioned archive-relative path for a backtest result."""
    source_root = _absolute_path(result_dir)
    _reject_symlink_components(source_root)
    config = _read_json_object_nofollow(source_root / "config.json", source_root, required=True)
    _read_json_object_nofollow(source_root / "analysis.json", source_root, required=True)
    fingerprint = directory_fingerprint(result_dir)
    version = config_version_info(config, fingerprint=fingerprint)
    config_name = _config_name_from_result(result_dir, config, archive_root)
    exchange_dir = _exchange_dir_from_config(config)
    result_name = safe_path_part(result_dir.name, "result")
    if not version["has_pb7_config_version"]:
        result_name = f"{result_name}__{fingerprint}"
    rel = (
        ARCHIVE_LAYOUT_ROOT
        / version["config_version"]
        / "backtests"
        / config_name
        / exchange_dir
        / result_name
    )
    meta = {
        **version,
        "type": "backtest_result",
        "config_name": config_name,
        "exchange_dir": exchange_dir,
        "result_name": result_name,
        "relative_path": rel.as_posix(),
    }
    return rel, meta


def _unique_path_for_collision(
    path: Path,
    fingerprint: str,
    identical: Callable[[Path], bool] | None = None,
) -> tuple[Path, bool]:
    """Return a free collision path or an existing identical fingerprint candidate."""
    base = path.with_name(f"{path.name}__{safe_path_part(fingerprint, 'copy')}")
    if not base.exists():
        return base, False
    if identical is not None and identical(base):
        return base, True
    index = 2
    while True:
        candidate = path.with_name(f"{path.name}__{safe_path_part(fingerprint, 'copy')}_{index}")
        if not candidate.exists():
            return candidate, False
        if identical is not None and identical(candidate):
            return candidate, True
        index += 1


def _valid_backtest_fingerprint(result_dir: Path, archive_root: Path) -> str | None:
    """Return a fingerprint only for a safe result with valid required JSON objects."""
    try:
        _validate_archive_path(result_dir, archive_root, require_exists=True)
        _read_json_object_nofollow(result_dir / "config.json", archive_root, required=True)
        _read_json_object_nofollow(result_dir / "analysis.json", archive_root, required=True)
        return directory_fingerprint(result_dir)
    except (OSError, RuntimeError, ValueError):
        return None


def _copy_backtest_result_to_archive_locked(source_dir: Path, archive_root: Path) -> dict:
    """Copy one result while its archive transaction lock is held."""
    source_dir = _validate_directory_tree_no_symlinks(source_dir)
    rel, meta = derive_backtest_archive_relative_path(source_dir, archive_root)
    dest = _validate_archive_path(archive_root / rel, archive_root)
    source_fingerprint = meta["fingerprint"]
    copied = True
    skipped = False
    if dest.exists():
        if _valid_backtest_fingerprint(dest, archive_root) == source_fingerprint:
            copied = False
            skipped = True
        else:
            dest, skipped = _unique_path_for_collision(
                dest,
                source_fingerprint,
                lambda candidate: _valid_backtest_fingerprint(candidate, archive_root) == source_fingerprint,
            )
            copied = not skipped
            meta["result_name"] = dest.name
            meta["relative_path"] = dest.relative_to(_absolute_path(archive_root)).as_posix()
    if copied:
        _ensure_archive_directory(dest.parent, archive_root)
        _validate_archive_path(dest, archive_root)
        staging: Path | None = Path(tempfile.mkdtemp(prefix=f".{dest.name}.stage-", dir=str(dest.parent)))
        try:
            shutil.copytree(str(source_dir), str(staging), symlinks=True, dirs_exist_ok=True)
            _validate_directory_tree_no_symlinks(staging)
            _read_json_object_nofollow(staging / "config.json", archive_root, required=True)
            _read_json_object_nofollow(staging / "analysis.json", archive_root, required=True)
            staged_fingerprint = directory_fingerprint(staging)
            _validate_directory_tree_no_symlinks(source_dir)
            _read_json_object_nofollow(source_dir / "config.json", source_dir, required=True)
            _read_json_object_nofollow(source_dir / "analysis.json", source_dir, required=True)
            current_source_fingerprint = directory_fingerprint(source_dir)
            if staged_fingerprint != source_fingerprint or current_source_fingerprint != source_fingerprint:
                raise RuntimeError("Backtest source changed while it was being copied")
            _validate_archive_path(dest, archive_root)
            if dest.exists() or dest.is_symlink():
                raise RuntimeError(f"Archive destination appeared during copy: {dest}")
            os.rename(staging, dest)
            staging = None
        finally:
            if staging is not None and (staging.exists() or staging.is_symlink()):
                if staging.is_symlink():
                    staging.unlink()
                else:
                    shutil.rmtree(staging)
                if staging.exists() or staging.is_symlink():
                    raise RuntimeError(f"Archive staging path still exists after cleanup: {staging}")
    return {"ok": True, "path": str(dest), "relative_path": meta["relative_path"], "skipped": skipped, "meta": meta}


def copy_backtest_result_to_archive(source_dir: Path, archive_root: Path) -> dict:
    """Copy a local backtest result into the versioned archive layout."""
    with archive_transaction(archive_root):
        return _copy_backtest_result_to_archive_locked(source_dir, archive_root)


def detect_liquidation(analysis: dict, config: dict) -> tuple[bool, str]:
    """Detect whether an analysis/config pair represents a liquidated result."""
    backtest = (config or {}).get("backtest") or {}
    drawdown = analysis.get("drawdown_worst_usd", analysis.get("drawdown_worst", 0))
    eqbal_diff = analysis.get("equity_balance_diff_neg_max_usd", analysis.get("equity_balance_diff_neg_max", 0))
    gain = analysis.get("gain_usd", analysis.get("gain", 0))
    starting_balance = backtest.get("starting_balance", 0)
    final_balance = starting_balance * gain if starting_balance else 0
    if "liquidated" in analysis:
        return bool(analysis["liquidated"]), "analysis.liquidated"
    try:
        if float(drawdown or 0) >= 0.95:
            return True, "drawdown_worst"
        if float(eqbal_diff or 0) >= 0.95:
            return True, "equity_balance_diff_neg_max"
        liq_threshold = float(backtest.get("liquidation_threshold", 0.05) or 0.05)
        if float(starting_balance or 0) > 0 and float(final_balance or 0) < float(starting_balance) * liq_threshold:
            return True, "final_balance"
    except (TypeError, ValueError):
        return False, ""
    return False, ""


def summarize_backtest_result(result_dir: Path, archive_root: Path) -> dict:
    """Return the UI/API summary for one archived backtest result directory."""
    result_dir = _validate_archive_path(result_dir, archive_root, require_exists=True)
    analysis_file = result_dir / "analysis.json"
    analysis = _read_json_object_nofollow(analysis_file, archive_root, required=True)
    config = _read_json_object_nofollow(result_dir / "config.json", archive_root, required=True)
    backtest = config.get("backtest", {}) if isinstance(config, dict) else {}
    bot = config.get("bot", {}) if isinstance(config, dict) else {}
    adg = analysis.get("adg_usd", analysis.get("adg", 0))
    drawdown = analysis.get("drawdown_worst_usd", analysis.get("drawdown_worst", 0))
    sharpe = analysis.get("sharpe_ratio_usd", analysis.get("sharpe_ratio", 0))
    eqbal_diff = analysis.get("equity_balance_diff_neg_max_usd", analysis.get("equity_balance_diff_neg_max", 0))
    gain = analysis.get("gain_usd", analysis.get("gain", 0))
    starting_balance = backtest.get("starting_balance", 0)
    final_balance = starting_balance * gain if starting_balance else 0
    liquidated, liquidation_reason = detect_liquidation(analysis, config)
    rel = result_dir.resolve().relative_to(archive_root.resolve())
    config_name = _config_name_from_result(result_dir, config, archive_root)
    coins = _coins_from_result_metadata(result_dir, config, archive_root)
    version = config_version_info(config)
    return {
        "path": str(result_dir),
        "display_name": rel.as_posix(),
        "config_name": config_name,
        "result_name": result_dir.name,
        "exchange_dir": _exchange_dir_from_config(config),
        "coins": coins,
        "coins_text": ", ".join(coins),
        "config_version": version["config_version"],
        "config_family": version["config_family"],
        "backtest_version": version["backtest_version"],
        "pb7_config_version": version["pb7_config_version"],
        "pbgui_version": version["pbgui_version"],
        "layout": "current" if is_new_backtest_result_path(result_dir, archive_root) else "legacy",
        "adg": adg,
        "drawdown_worst": drawdown,
        "sharpe_ratio": sharpe,
        "equity_balance_diff_neg_max": eqbal_diff,
        "gain": gain,
        "starting_balance": starting_balance,
        "final_balance": final_balance,
        "liquidated": liquidated,
        "liquidation_reason": liquidation_reason,
        "exchanges": backtest.get("exchanges", []),
        "start_date": backtest.get("start_date", ""),
        "end_date": backtest.get("end_date", ""),
        "btc_collateral_cap": float(backtest.get("btc_collateral_cap") or 0),
        "twe_long": (bot.get("long", {}).get("risk", {}) if version["backtest_version"] == "v8" else bot.get("long", {})).get("total_wallet_exposure_limit", 0),
        "twe_short": (bot.get("short", {}).get("risk", {}) if version["backtest_version"] == "v8" else bot.get("short", {})).get("total_wallet_exposure_limit", 0),
        "pos_long": (bot.get("long", {}).get("risk", {}) if version["backtest_version"] == "v8" else bot.get("long", {})).get("n_positions", 0),
        "pos_short": (bot.get("short", {}).get("risk", {}) if version["backtest_version"] == "v8" else bot.get("short", {})).get("n_positions", 0),
        "modified": datetime.datetime.fromtimestamp(analysis_file.stat().st_mtime).isoformat(),
        "analysis": analysis,
    }


def list_archive_backtest_results(archive_root: Path) -> list[dict]:
    """List archived backtest results across current and legacy layouts."""
    results = []
    try:
        archive_root = _validate_archive_path(archive_root, archive_root)
    except RuntimeError:
        return results
    if not archive_root.exists():
        return results
    for analysis_file in sorted(archive_root.glob("**/analysis.json")):
        if ".git" in analysis_file.parts:
            continue
        try:
            result_dir = _validate_archive_path(analysis_file.parent, archive_root, require_exists=True)
            results.append(summarize_backtest_result(result_dir, archive_root))
        except Exception:
            continue
    return results


def _to_float(value: Any) -> float | None:
    """Return a finite float or None."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _metric_from_analysis(analysis: dict, spec: dict) -> tuple[float | None, str]:
    """Resolve one score metric from exact keys or fallback prefixes."""
    for key in spec.get("keys") or []:
        if key in analysis:
            value = _to_float(analysis.get(key))
            if value is not None:
                return value, key
    prefixed: list[tuple[float, str]] = []
    for prefix in spec.get("prefixes") or []:
        for key, raw in analysis.items():
            if str(key).startswith(prefix):
                value = _to_float(raw)
                if value is not None:
                    prefixed.append((value, str(key)))
    if not prefixed:
        return None, ""
    # For grouped fallback metrics, use the strongest/worst observed value as representative.
    value, key = max(prefixed, key=lambda item: item[0])
    return value, key


def _quantile(values: list[float], q: float) -> float:
    """Return a simple linear quantile for sorted or unsorted finite values."""
    vals = sorted(v for v in values if math.isfinite(v))
    if not vals:
        return 0.0
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * max(0.0, min(1.0, q))
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return vals[lo]
    return vals[lo] + (vals[hi] - vals[lo]) * (pos - lo)


def _metric_bounds(values: list[float]) -> tuple[float, float]:
    """Return robust percentile bounds for metric normalization."""
    vals = [v for v in values if math.isfinite(v)]
    if not vals:
        return 0.0, 0.0
    lo = _quantile(vals, 0.05)
    hi = _quantile(vals, 0.95)
    if hi <= lo:
        lo = min(vals)
        hi = max(vals)
    return lo, hi


def _normalize_metric(value: float | None, bounds: tuple[float, float], *, lower_is_better: bool) -> float | None:
    """Normalize one metric to 0..1 using robust bounds."""
    if value is None:
        return None
    lo, hi = bounds
    if hi <= lo:
        score = 0.5
    else:
        score = (value - lo) / (hi - lo)
    score = max(0.0, min(1.0, score))
    if lower_is_better:
        score = 1.0 - score
    return score


def _result_duration_days(result: dict) -> int | None:
    """Return configured backtest duration in days when parseable."""
    start = str(result.get("start_date") or "")[:10]
    end = str(result.get("end_date") or "")[:10]
    try:
        start_date = datetime.date.fromisoformat(start)
        end_date = datetime.date.fromisoformat(end)
    except ValueError:
        return None
    days = (end_date - start_date).days + 1
    return days if days > 0 else None


def _primary_analysis_metric(result: dict, keys: list[str], default: float | None = None) -> float | None:
    """Return a primary analysis metric from result.analysis with top-level fallback."""
    analysis = result.get("analysis") if isinstance(result.get("analysis"), dict) else {}
    for key in keys:
        value = _to_float(analysis.get(key))
        if value is not None:
            return value
        value = _to_float(result.get(key))
        if value is not None:
            return value
    return default


def _data_quality_score(result: dict, coverage: float) -> tuple[float, list[str]]:
    """Return data-quality score 0..1 and flags."""
    flags = []
    duration = _result_duration_days(result)
    if duration is None:
        duration_score = 0.55
        flags.append("unknown_duration")
    elif duration < 30:
        duration_score = 0.30
        flags.append("very_short_backtest")
    elif duration < 90:
        duration_score = 0.55
        flags.append("short_backtest")
    elif duration < 180:
        duration_score = 0.75
    else:
        duration_score = 1.0
    completion = _primary_analysis_metric(result, ["backtest_completion_ratio"], 1.0)
    completion_score = max(0.0, min(1.0, completion if completion is not None else 0.75))
    if completion_score < 0.95:
        flags.append("incomplete_backtest")
    quality = 0.45 * completion_score + 0.35 * duration_score + 0.20 * max(0.0, min(1.0, coverage))
    return max(0.0, min(1.0, quality)), flags


def _score_cap(result: dict) -> tuple[float, list[str]]:
    """Return maximum score based on hard safety caps."""
    flags = []
    if result.get("liquidated"):
        return 1.0, ["liquidated"]
    cap = 10.0
    drawdown = _primary_analysis_metric(result, ["drawdown_worst_w_usd", "drawdown_worst_usd", "drawdown_worst_w", "drawdown_worst"], 0.0) or 0.0
    equity_diff = _primary_analysis_metric(result, ["equity_balance_diff_neg_max_usd", "equity_balance_diff_neg_max", "equity_balance_diff_max_usd", "equity_balance_diff_max"], 0.0) or 0.0
    if drawdown >= 0.95:
        cap = min(cap, 2.0)
        flags.append("extreme_drawdown")
    elif drawdown >= 0.70:
        cap = min(cap, 4.0)
        flags.append("very_high_drawdown")
    elif drawdown >= 0.50:
        cap = min(cap, 6.0)
        flags.append("high_drawdown")
    elif drawdown >= 0.40:
        cap = min(cap, 6.5)
        flags.append("elevated_drawdown")
    elif drawdown >= 0.30:
        cap = min(cap, 7.0)
        flags.append("moderate_drawdown")
    if equity_diff >= 0.95:
        cap = min(cap, 2.0)
        flags.append("extreme_equity_balance_diff")
    elif equity_diff >= 0.70:
        cap = min(cap, 4.0)
        flags.append("very_high_equity_balance_diff")
    elif equity_diff >= 0.50:
        cap = min(cap, 6.0)
        flags.append("high_equity_balance_diff")
    elif equity_diff >= 0.40:
        cap = min(cap, 6.5)
        flags.append("elevated_equity_balance_diff")
    elif equity_diff >= 0.30:
        cap = min(cap, 7.0)
        flags.append("moderate_equity_balance_diff")
    duration = _result_duration_days(result)
    if duration is not None and duration < 30:
        cap = min(cap, 5.0)
    elif duration is not None and duration < 90:
        cap = min(cap, 7.0)
    completion = _primary_analysis_metric(result, ["backtest_completion_ratio"], 1.0) or 1.0
    if completion < 0.80:
        cap = min(cap, 4.0)
    elif completion < 0.95:
        cap = min(cap, 6.0)
    return cap, flags


def _score_to_ten(score: float | None) -> float | None:
    """Convert 0..1 score to 1..10."""
    if score is None:
        return None
    return round(1.0 + max(0.0, min(1.0, score)) * 9.0, 1)


def score_archive_results(results: list[dict]) -> list[dict]:
    """Attach PBGui score details to archive result summaries."""
    generated_at = utc_now_iso()
    prepared = [dict(result) for result in results]
    values_by_metric: list[list[float]] = [[] for _ in _SCORE_METRICS]
    resolved_values: list[list[tuple[float | None, str]]] = []
    for result in prepared:
        analysis = result.get("analysis") if isinstance(result.get("analysis"), dict) else {}
        resolved = []
        for idx, spec in enumerate(_SCORE_METRICS):
            value, key = _metric_from_analysis(analysis, spec)
            resolved.append((value, key))
            if value is not None:
                values_by_metric[idx].append(value)
        resolved_values.append(resolved)
    bounds_by_metric = [_metric_bounds(values) for values in values_by_metric]
    total_metric_weight = sum(_SCORE_GROUP_WEIGHTS.get(str(spec.get("group")), 0.0) for spec in _SCORE_METRICS)

    for result, resolved in zip(prepared, resolved_values):
        group_values: dict[str, list[float]] = {group: [] for group in _SCORE_GROUP_WEIGHTS}
        used_metrics = []
        available_weight = 0.0
        for idx, spec in enumerate(_SCORE_METRICS):
            value, key = resolved[idx]
            metric_score = _normalize_metric(value, bounds_by_metric[idx], lower_is_better=bool(spec.get("lower")))
            if metric_score is None:
                continue
            group = str(spec.get("group") or "")
            group_values.setdefault(group, []).append(metric_score)
            available_weight += _SCORE_GROUP_WEIGHTS.get(group, 0.0)
            used_metrics.append({"label": spec.get("label", key), "key": key, "value": value, "score": round(metric_score, 4)})
        coverage = available_weight / total_metric_weight if total_metric_weight > 0 else 0.0
        data_quality, data_flags = _data_quality_score(result, coverage)
        group_values["data_quality"] = [data_quality]

        parts_01 = {}
        for group in _SCORE_GROUP_WEIGHTS:
            values = group_values.get(group) or []
            parts_01[group] = sum(values) / len(values) if values else 0.5
        base = sum(parts_01[group] * weight for group, weight in _SCORE_GROUP_WEIGHTS.items())
        score_value = _score_to_ten(base) or 1.0
        cap, cap_flags = _score_cap(result)
        flags = [*data_flags, *cap_flags]
        if score_value > cap:
            score_value = cap
            if cap > 1.0:
                flags.append("score_capped")
        confidence = round(max(0.10, min(1.0, 0.25 + 0.75 * coverage)) * (0.75 + 0.25 * data_quality), 3)
        if result.get("liquidated"):
            confidence = max(confidence, 0.9)
        score = {
            "version": ARCHIVE_SCORE_VERSION,
            "value": round(score_value, 1),
            "confidence": confidence,
            "parts": {group: _score_to_ten(value) for group, value in parts_01.items()},
            "flags": sorted(set(flags)),
            "coverage": round(coverage, 3),
            "metrics_used": used_metrics,
            "calculated_at": generated_at,
        }
        result["pbgui_score"] = score
    prepared.sort(key=lambda item: (-(item.get("pbgui_score") or {}).get("value", 0), str(item.get("display_name") or "")))
    return prepared


def _format_score_metric(value: Any, decimals: int = 4) -> str:
    """Format score table metric cells."""
    number = _to_float(value)
    if number is None:
        return ""
    return f"{number:.{decimals}f}"


def _format_score_table_text(value: Any, break_tag: str = "<wbr>", wrap_at: int = 0) -> str:
    """Format text cells so long strategy names can wrap in GitHub tables."""
    raw = str(value or "").replace("\n", " ")
    if wrap_at > 0:
        chunks = []
        line_len = 0
        for char in raw:
            chunks.append("&#124;" if char == "|" else html.escape(char, quote=False))
            line_len += 1
            if char in "_./-" and line_len >= wrap_at:
                chunks.append("<br>")
                line_len = 0
        return "".join(chunks)
    text = html.escape(raw.replace("|", "&#124;"), quote=False)
    if not break_tag:
        return text
    return re.sub(r"([_./-])", lambda match: match.group(1) + break_tag, text)


def _format_score_config_link(row: dict) -> str:
    """Return the linked config name for the archived result folder."""
    path = str(row.get("path") or "")
    label = _format_score_table_text(row.get("config_name") or row.get("result_name") or path)
    if path:
        title = html.escape(str(row.get("result_name") or path), quote=True)
        return f'<a href="{html.escape(quote(path), quote=True)}" title="{title}">{label}</a>'
    return label


def archive_score_rows(scored_results: list[dict], limit: int = 100) -> list[dict]:
    """Return compact score rows for API previews."""
    rows = []
    for result in scored_results[:max(1, limit)]:
        score = result.get("pbgui_score") or {}
        rows.append({
            "score": score.get("value"),
            "parts": score.get("parts", {}),
            "flags": score.get("flags", []),
            "config_name": result.get("config_name", ""),
            "coins": result.get("coins", []),
            "coins_text": result.get("coins_text", ""),
            "exchange_dir": result.get("exchange_dir", ""),
            "result_name": result.get("result_name", ""),
            "path": result.get("display_name", ""),
            "adg": result.get("adg"),
            "gain": result.get("gain"),
            "drawdown_worst": result.get("drawdown_worst"),
            "sharpe_ratio": result.get("sharpe_ratio"),
        })
    return rows


def build_archive_scores_markdown(scored_results: list[dict], limit: int | None = None) -> str:
    """Build GitHub-readable Markdown for archive score overview."""
    if not scored_results:
        return README_SCORES_PLACEHOLDER
    rows = archive_score_rows(scored_results, limit=len(scored_results) if limit is None else limit)
    lines = [
        "## PBGui Score Overview",
        "",
        f"Score version: `{ARCHIVE_SCORE_VERSION}`  ",
        f"Results scored: `{len(scored_results)}`",
        "",
        '<table width="100%">',
        "<thead>",
        "<tr>",
        '<th align="right" width="7%">Score</th>',
        '<th align="right" width="11%">ADG</th>',
        '<th align="right" width="11%">Gain</th>',
        '<th align="right" width="9%">DD</th>',
        '<th align="right" width="10%">Sharpe</th>',
        '<th width="52%">Config</th>',
        "</tr>",
        "</thead>",
        "<tbody>",
    ]
    for row in rows:
        flags = ", ".join(_format_score_table_text(flag) for flag in row.get("flags") or [])
        config = _format_score_config_link(row)
        if flags:
            config += f"<br>Flags: {flags}"
        lines.append(
            "<tr>"
            f'<td align="right">{html.escape(_format_score_metric(row.get("score"), 1), quote=False)}</td>'
            f'<td align="right">{html.escape(_format_score_metric(row.get("adg"), 6), quote=False)}</td>'
            f'<td align="right">{html.escape(_format_score_metric(row.get("gain"), 4), quote=False)}</td>'
            f'<td align="right">{html.escape(_format_score_metric(row.get("drawdown_worst"), 4), quote=False)}</td>'
            f'<td align="right">{html.escape(_format_score_metric(row.get("sharpe_ratio"), 4), quote=False)}</td>'
            f"<td>{config}</td>"
            "</tr>"
        )
    lines.extend(["</tbody>", "</table>"])
    return "\n".join(lines)


def build_archive_scores_html(scored_results: list[dict], archive_root: Path | None = None) -> str:
    """Build a standalone sortable HTML score table."""
    rows = archive_score_rows(scored_results, limit=len(scored_results))
    payload = []
    for row in rows:
        score = row.get("score")
        path = str(row.get("path") or "")
        url = archive_github_tree_url(archive_root, path) if archive_root is not None else ""
        payload.append({
            "score": score,
            "adg": row.get("adg"),
            "gain": row.get("gain"),
            "drawdown_worst": row.get("drawdown_worst"),
            "sharpe_ratio": row.get("sharpe_ratio"),
            "config_name": str(row.get("config_name") or ""),
            "coins": [str(coin) for coin in row.get("coins") or []],
            "coins_text": str(row.get("coins_text") or ""),
            "result_name": str(row.get("result_name") or ""),
            "exchange_dir": str(row.get("exchange_dir") or ""),
            "path": path,
            "url": url or path,
            "flags": [str(flag) for flag in row.get("flags") or []],
            "score_text": _format_score_metric(score, 1),
            "adg_text": _format_score_metric(row.get("adg"), 6),
            "gain_text": _format_score_metric(row.get("gain"), 4),
            "drawdown_worst_text": _format_score_metric(row.get("drawdown_worst"), 4),
            "sharpe_ratio_text": _format_score_metric(row.get("sharpe_ratio"), 4),
        })
    data_json = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).replace("</", "<\\/")
    generated = utc_now_iso()
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PBGui Score Table</title>
<style>
:root {{
  --bg: #0d1117;
  --panel: #161b22;
  --panel2: #21262d;
  --border: #30363d;
  --text: #e6edf3;
  --muted: #8b949e;
  --accent: #58a6ff;
  --row: #0d1117;
  --row-alt: #111820;
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--bg); color: var(--text); font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
a {{ color: var(--accent); text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
.page {{ width: min(100vw, 1800px); margin: 0 auto; padding: 20px 24px 36px; }}
.top {{ display: flex; gap: 16px; justify-content: space-between; align-items: flex-start; margin-bottom: 18px; }}
h1 {{ margin: 0 0 6px; font-size: 28px; }}
.meta {{ color: var(--muted); font-size: 13px; }}
.toolbar {{ display: flex; gap: 10px; align-items: center; margin: 16px 0; flex-wrap: wrap; }}
.toolbar label {{ color: var(--muted); font-size: 13px; }}
input[type="search"], select {{ padding: 9px 11px; border: 1px solid var(--border); border-radius: 8px; background: var(--panel); color: var(--text); }}
input[type="search"] {{ flex: 1 1 320px; max-width: 620px; }}
select {{ min-width: 150px; }}
.count {{ color: var(--muted); }}
.table-wrap {{ width: 100%; overflow-x: auto; border: 1px solid var(--border); border-radius: 10px; background: var(--panel); }}
table {{ width: 100%; min-width: 1220px; border-collapse: collapse; }}
th, td {{ padding: 8px 10px; border-bottom: 1px solid var(--border); vertical-align: top; }}
th {{ position: sticky; top: 0; background: var(--panel2); color: #f0f6fc; text-align: left; white-space: nowrap; cursor: pointer; user-select: none; }}
th.num, td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
tbody tr:nth-child(even) {{ background: var(--row-alt); }}
tbody tr:nth-child(odd) {{ background: var(--row); }}
.config {{ min-width: 360px; overflow-wrap: anywhere; }}
.sub {{ color: var(--muted); font-size: 12px; margin-top: 2px; }}
.flag {{ display: inline-block; color: #ffb86c; margin-left: 6px; }}
.sort::after {{ content: ""; color: var(--muted); margin-left: 6px; }}
.sort.asc::after {{ content: "up"; }}
.sort.desc::after {{ content: "down"; }}
@media (max-width: 760px) {{ .page {{ padding: 14px; }} .top {{ display: block; }} }}
</style>
</head>
<body>
<main class="page">
  <div class="top">
    <div>
      <h1>PBGui Score Table</h1>
      <div class="meta">Score version {ARCHIVE_SCORE_VERSION} - {len(scored_results)} results - generated {html.escape(generated)}</div>
    </div>
    <div><a href="README.md">Back to README</a> - <a href="SCORES.md">Markdown table</a></div>
  </div>
  <div class="toolbar">
    <input id="filter" type="search" placeholder="Filter config, result, exchange, flags..." autocomplete="off">
    <label for="coin-filter">Coin</label>
    <select id="coin-filter"><option value="">All coins</option></select>
    <span id="count" class="count"></span>
  </div>
  <div class="table-wrap">
    <table id="scores">
      <thead>
        <tr>
          <th class="num sort" data-key="score">Score</th>
          <th class="num sort" data-key="adg">ADG</th>
          <th class="num sort" data-key="gain">Gain</th>
          <th class="num sort" data-key="drawdown_worst">DD</th>
          <th class="num sort" data-key="sharpe_ratio">Sharpe</th>
          <th class="sort" data-key="coins_text">Coins</th>
          <th class="sort" data-key="config_name">Config</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</main>
<script id="score-data" type="application/json">{data_json}</script>
<script>
const rows = JSON.parse(document.getElementById('score-data').textContent || '[]');
const tbody = document.querySelector('#scores tbody');
const filterInput = document.getElementById('filter');
const coinFilter = document.getElementById('coin-filter');
const countEl = document.getElementById('count');
let sortKey = 'score';
let sortDir = -1;

function esc(value) {{
  return String(value == null ? '' : value).replace(/[&<>"']/g, ch => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[ch]));
}}

function numeric(value) {{
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}}

function compareRows(a, b) {{
  const av = a[sortKey];
  const bv = b[sortKey];
  const an = numeric(av);
  const bn = numeric(bv);
  if (an !== null || bn !== null) return ((an ?? -Infinity) - (bn ?? -Infinity)) * sortDir;
  return String(av || '').localeCompare(String(bv || '')) * sortDir;
}}

function populateCoinFilter() {{
  const coins = [];
  rows.forEach(row => (row.coins || []).forEach(coin => {{
    if (coin && !coins.includes(coin)) coins.push(coin);
  }}));
  coins.sort((a, b) => String(a).localeCompare(String(b)));
  coinFilter.innerHTML = '<option value="">All coins</option>' + coins.map(coin => '<option value="' + esc(coin) + '">' + esc(coin) + '</option>').join('');
}}

function rowMatches(row, term, coin) {{
  if (coin && !(row.coins || []).includes(coin)) return false;
  if (!term) return true;
  const hay = [row.config_name, row.result_name, row.exchange_dir, row.coins_text, (row.flags || []).join(' ')].join(' ').toLowerCase();
  return hay.includes(term);
}}

function render() {{
  const term = filterInput.value.trim().toLowerCase();
  const coin = coinFilter.value;
  const filtered = rows.filter(row => rowMatches(row, term, coin)).sort(compareRows);
  tbody.innerHTML = filtered.map(row => {{
    const flags = (row.flags || []).map(flag => '<span class="flag">' + esc(flag) + '</span>').join('');
    const href = row.url || row.path || '';
    const link = href ? '<a href="' + encodeURI(href) + '">' + esc(row.config_name || row.result_name || row.path) + '</a>' : esc(row.config_name || row.result_name || '');
    return '<tr>'
      + '<td class="num">' + esc(row.score_text) + '</td>'
      + '<td class="num">' + esc(row.adg_text) + '</td>'
      + '<td class="num">' + esc(row.gain_text) + '</td>'
      + '<td class="num">' + esc(row.drawdown_worst_text) + '</td>'
      + '<td class="num">' + esc(row.sharpe_ratio_text) + '</td>'
      + '<td>' + esc(row.coins_text) + '</td>'
      + '<td class="config">' + link + flags + '<div class="sub">' + esc(row.exchange_dir) + ' - ' + esc(row.result_name) + '</div></td>'
      + '</tr>';
  }}).join('');
  countEl.textContent = filtered.length + ' / ' + rows.length + ' rows';
  document.querySelectorAll('th.sort').forEach(th => {{
    th.classList.toggle('asc', th.dataset.key === sortKey && sortDir === 1);
    th.classList.toggle('desc', th.dataset.key === sortKey && sortDir === -1);
  }});
}}

document.querySelectorAll('th.sort').forEach(th => {{
  th.addEventListener('click', () => {{
    if (sortKey === th.dataset.key) sortDir *= -1;
    else {{ sortKey = th.dataset.key; sortDir = th.classList.contains('num') ? -1 : 1; }}
    render();
  }});
}});
filterInput.addEventListener('input', render);
coinFilter.addEventListener('change', render);
populateCoinFilter();
render();
</script>
</body>
</html>
"""


def build_archive_scores_summary_markdown(scored_results: list[dict], archive_root: Path | None = None) -> str:
    """Build the compact README score section with a link to the full table."""
    if not scored_results:
        return README_SCORES_PLACEHOLDER
    html_link = archive_github_pages_url(archive_root) if archive_root is not None else ""
    if not html_link:
        html_link = ARCHIVE_SCORES_HTML.as_posix()
    lines = [
        "## PBGui Score Overview",
        "",
        f"Score version: `{ARCHIVE_SCORE_VERSION}`  ",
        f"Results scored: `{len(scored_results)}`",
        "",
        "The complete score table is available as an interactive sortable HTML page and a Markdown fallback:",
        "",
        f"[Open interactive score table]({html_link})  ",
        f"[Open Markdown score table]({ARCHIVE_SCORES.as_posix()})",
    ]
    top_rows = archive_score_rows(scored_results, limit=min(10, len(scored_results)))
    if top_rows:
        lines.extend([
            "",
            "### Top 10 Scores",
            "",
            "| Score | Config |",
            "|---:|---|",
        ])
        for row in top_rows:
            path = str(row.get("path") or "")
            label = str(row.get("config_name") or row.get("result_name") or path).replace("|", "\\|")
            config = f"[{label}]({quote(path)})" if path else label
            lines.append(f"| {_format_score_metric(row.get('score'), 1)} | {config} |")
    return "\n".join(lines)


def build_archive_scores_page_markdown(scored_results: list[dict], archive_root: Path | None = None) -> str:
    """Build the standalone full score table Markdown page."""
    html_link = archive_github_pages_url(archive_root) if archive_root is not None else ""
    if not html_link:
        html_link = ARCHIVE_SCORES_HTML.as_posix()
    return "\n".join([
        "# PBGui Score Table",
        "",
        f"[Open interactive score table]({html_link})  ",
        "[Back to README](README.md)",
        "",
        build_archive_scores_markdown(scored_results),
        "",
    ])


def build_archive_score_payload(archive_root: Path, limit: int = 100) -> dict:
    """Return read-only archive score preview payload."""
    scored = score_archive_results(list_archive_backtest_results(archive_root))
    rows = archive_score_rows(scored, limit=limit)
    markdown = build_archive_scores_markdown(scored)
    readme_markdown = build_archive_readme_content(archive_root, scores_markdown=build_archive_scores_summary_markdown(scored, archive_root))
    scores_page_markdown = build_archive_scores_page_markdown(scored, archive_root)
    scores_page_html = build_archive_scores_html(scored, archive_root)
    return {
        "ok": True,
        "score_version": ARCHIVE_SCORE_VERSION,
        "generated_at": utc_now_iso(),
        "total": len(scored),
        "scored": len([r for r in scored if r.get("pbgui_score")]),
        "rows": rows,
        "markdown": markdown,
        "readme_markdown": readme_markdown,
        "scores_page_markdown": scores_page_markdown,
        "scores_page_html": scores_page_html,
        "scores_path": ARCHIVE_SCORES.as_posix(),
        "scores_html_path": ARCHIVE_SCORES_HTML.as_posix(),
    }


def _iter_legacy_result_dirs(archive_root: Path):
    """Yield safe legacy result directories lazily."""
    try:
        archive_root = _validate_archive_path(archive_root, archive_root)
    except RuntimeError:
        return
    if not archive_root.exists():
        return
    for analysis_file in archive_root.glob("**/analysis.json"):
        if ".git" in analysis_file.parts:
            continue
        try:
            result_dir = _validate_archive_path(analysis_file.parent, archive_root, require_exists=True)
            _read_json_object_nofollow(result_dir / "config.json", archive_root, required=True)
            _read_json_object_nofollow(result_dir / "analysis.json", archive_root, required=True)
        except (OSError, RuntimeError, ValueError):
            continue
        if not is_new_backtest_result_path(result_dir, archive_root):
            yield result_dir


def legacy_result_dirs(archive_root: Path) -> list[Path]:
    """Return safe legacy result directories that need layout migration."""
    return sorted(_iter_legacy_result_dirs(archive_root))


def git_worktree_state(archive_root: Path) -> dict:
    """Return simple git status information for an archive clone."""
    if not (archive_root / ".git").exists():
        return {"is_git": False, "dirty": False, "porcelain": "", "status_ok": True, "returncode": None}
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=str(archive_root),
            capture_output=True,
            text=True,
            timeout=15,
        )
    except Exception:
        return {"is_git": True, "dirty": True, "porcelain": "", "status_ok": False, "returncode": None}
    porcelain = (result.stdout or "").strip()
    status_ok = result.returncode == 0
    return {
        "is_git": True,
        "dirty": not status_ok or bool(porcelain),
        "porcelain": porcelain,
        "status_ok": status_ok,
        "returncode": result.returncode,
    }


def cleanup_empty_parents(path: Path, stop_at: Path) -> None:
    """Remove empty parent directories up to but not including stop_at."""
    stop = _validate_archive_path(stop_at, stop_at)
    parent = _validate_archive_path(path, stop).parent
    while parent != stop and parent.is_dir() and not parent.is_symlink():
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent


def _safe_nonnegative_int(value: Any) -> int:
    """Return a nonnegative integer for untrusted report counters."""
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def _porcelain_sha256(porcelain: str) -> str:
    """Return the exact SHA256 digest of a git porcelain string."""
    return hashlib.sha256(str(porcelain).encode("utf-8")).hexdigest()


def archive_migration_status(archive_root: Path, fast: bool = False) -> dict:
    """Return compact layout migration status for UI display."""
    git_state = git_worktree_state(archive_root)
    report = _read_json_object_nofollow(archive_root / ARCHIVE_REPORT, archive_root) or {}
    migrated = _safe_nonnegative_int(report.get("migrated"))
    removed_duplicates = _safe_nonnegative_int(report.get("removed_duplicates"))
    legacy_git_state = "status_ok" not in git_state
    status_ok = bool(git_state.get("status_ok", legacy_git_state))
    report_hash = report.get("post_migration_porcelain_sha256")
    current_hash = _porcelain_sha256(git_state.get("porcelain", "")) if status_ok else ""
    if legacy_git_state and report_hash is None:
        report_hash = current_hash
    if not status_ok:
        return {
            "status": "git_status_failed",
            "label": "Archive layout: git status failed",
            "legacy_count": 0,
            "git": git_state,
            "report": report,
        }
    if migrated + removed_duplicates > 0 and git_state["dirty"] and isinstance(report_hash, str) and report_hash == current_hash:
        status = "migrated_pending_push"
        remaining = bool(report.get("remaining_legacy"))
        label = "Archive layout: migrated locally, push pending"
        if remaining:
            label += "; legacy entries remain"
        return {"status": status, "label": label, "legacy_count": 1 if remaining else 0, "git": git_state, "report": report}

    if fast:
        manifest = load_archive_manifest(archive_root)
        if manifest:
            status = "current"
            label = "Archive layout: current"
        else:
            status = "unknown"
            label = "Archive layout: not scanned"
        return {"status": status, "label": label, "legacy_count": 0, "git": git_state, "report": report}

    legacy_count = len(legacy_result_dirs(archive_root))
    if legacy_count > 0 and git_state["dirty"]:
        status = "migration_skipped_dirty"
        label = "Archive layout: migration skipped, dirty worktree"
    elif legacy_count > 0:
        status = "legacy_entries_detected"
        label = "Archive layout: legacy entries detected"
    else:
        status = "current"
        label = "Archive layout: current"
    return {"status": status, "label": label, "legacy_count": legacy_count, "git": git_state, "report": report}


def _migrate_archive_layout_locked(archive_root: Path, max_items: int | None = None) -> dict:
    """Move legacy results while the archive transaction lock is held."""
    try:
        archive_root = _validate_archive_path(archive_root, archive_root, require_exists=True)
    except RuntimeError as exc:
        return {"ok": False, "skipped": True, "reason": "unsafe_archive", "error": str(exc), "migrated": 0, "removed_duplicates": 0, "collisions": 0, "failed": 0, "skipped_items": 0, "remaining_legacy": False, "items": []}
    git_state = git_worktree_state(archive_root)
    if not git_state["is_git"]:
        return {"ok": False, "skipped": True, "reason": "not_git", "git": git_state, "migrated": 0, "removed_duplicates": 0, "collisions": 0, "failed": 0, "skipped_items": 0, "remaining_legacy": False, "items": []}
    if not git_state.get("status_ok", False):
        return {"ok": False, "skipped": True, "reason": "git_status_failed", "git": git_state, "migrated": 0, "removed_duplicates": 0, "collisions": 0, "failed": 0, "skipped_items": 0, "remaining_legacy": False, "items": []}
    if git_state["dirty"]:
        return {"ok": False, "skipped": True, "reason": "dirty_worktree", "git": git_state, "migrated": 0, "removed_duplicates": 0, "collisions": 0, "failed": 0, "skipped_items": 0, "remaining_legacy": False, "items": []}
    items = []
    migrated = 0
    removed_duplicates = 0
    collisions = 0
    failed = 0
    skipped_items = 0
    limit = None if max_items is None else int(max_items)
    candidates = _iter_legacy_result_dirs(archive_root)
    selected = list(candidates) if limit is None else list(islice(candidates, limit + 1))
    truncated = limit is not None and len(selected) > limit
    if truncated:
        selected = selected[:limit]
    for result_dir in selected:
        if not result_dir.exists():
            skipped_items += 1
            items.append({"source": str(result_dir), "target": "", "action": "skipped", "outcome": "missing_source"})
            continue
        try:
            rel, meta = derive_backtest_archive_relative_path(result_dir, archive_root)
            dest = archive_root / rel
            source_fingerprint = meta["fingerprint"]
            action = "moved"
            dest = _validate_archive_path(dest, archive_root)
            if dest.exists():
                if _valid_backtest_fingerprint(dest, archive_root) == source_fingerprint:
                    shutil.rmtree(str(result_dir))
                    if result_dir.exists():
                        raise RuntimeError(f"Duplicate source still exists after removal: {result_dir}")
                    cleanup_empty_parents(result_dir, archive_root)
                    removed_duplicates += 1
                    action = "removed_duplicate"
                    items.append({"source": str(result_dir), "target": str(dest), "action": action})
                    continue
                dest, identical = _unique_path_for_collision(
                    dest,
                    source_fingerprint,
                    lambda candidate: _valid_backtest_fingerprint(candidate, archive_root) == source_fingerprint,
                )
                if identical:
                    shutil.rmtree(str(result_dir))
                    if result_dir.exists():
                        raise RuntimeError(f"Duplicate source still exists after removal: {result_dir}")
                    cleanup_empty_parents(result_dir, archive_root)
                    removed_duplicates += 1
                    items.append({"source": str(result_dir), "target": str(dest), "action": "removed_duplicate"})
                    continue
                collisions += 1
                action = "moved_with_suffix"
            _ensure_archive_directory(dest.parent, archive_root)
            _validate_archive_path(dest, archive_root)
            shutil.move(str(result_dir), str(dest))
            cleanup_empty_parents(result_dir, archive_root)
            migrated += 1
            items.append({"source": str(result_dir), "target": str(dest), "action": action})
        except Exception as exc:
            failed += 1
            items.append({"source": str(result_dir), "target": "", "action": "skipped", "outcome": "failed", "error": str(exc)})
    report = {
        "ok": failed == 0,
        "skipped": False,
        "migrated": migrated,
        "removed_duplicates": removed_duplicates,
        "collisions": collisions,
        "failed": failed,
        "skipped_items": skipped_items,
        "items": items,
        "truncated": truncated,
        "remaining_legacy": truncated or failed > 0,
        "created_at": utc_now_iso(),
    }
    if migrated or removed_duplicates:
        if limit is None:
            report["manifest"] = rebuild_archive_manifest(archive_root)
        else:
            report["manifest"] = {"skipped": True, "reason": "bounded_migration"}
        _atomic_write_archive_json(archive_root / ARCHIVE_REPORT, report, archive_root)
        post_state = git_worktree_state(archive_root)
        if post_state.get("status_ok"):
            report["post_migration_porcelain_sha256"] = _porcelain_sha256(post_state.get("porcelain", ""))
            _atomic_write_archive_json(archive_root / ARCHIVE_REPORT, report, archive_root)
        else:
            report["post_migration_git_status_failed"] = True
            _atomic_write_archive_json(archive_root / ARCHIVE_REPORT, report, archive_root)
    return report


def migrate_archive_layout(archive_root: Path, max_items: int | None = None) -> dict:
    """Move legacy backtest results into the versioned archive layout when safe."""
    if max_items is not None and int(max_items) <= 0:
        raise ValueError("max_items must be greater than zero")
    with archive_transaction(archive_root):
        return _migrate_archive_layout_locked(archive_root, max_items=max_items)


def maybe_migrate_own_archive(
    archive_name: str,
    archive_root: Path,
    own_archive: str,
    max_items: int | None = None,
) -> dict:
    """Run automatic migration only for the configured own archive."""
    if not own_archive or archive_name != own_archive:
        return {"ran": False, "reason": "not_own_archive", "status": archive_migration_status(archive_root)}
    result = migrate_archive_layout(archive_root, max_items=max_items)
    return {"ran": not result.get("skipped"), "result": result, "status": archive_migration_status(archive_root)}


def derive_optimize_archive_relative_path(config_name: str, config: dict) -> tuple[Path, dict]:
    """Derive archive path metadata for an optimize config JSON file."""
    fingerprint = json_fingerprint(config or {})
    version = config_version_info(config or {}, fingerprint=fingerprint)
    stem = safe_path_part(config_name, "optimize_config")
    if not version["has_config_version"]:
        stem = f"{stem}__{fingerprint}"
    rel = ARCHIVE_LAYOUT_ROOT / version["config_version"] / "optimize" / f"{stem}.json"
    meta = {
        **version,
        "schema_version": 1,
        "type": "optimize_config",
        "name": config_name,
        "source": "pbgui",
        "created_at": utc_now_iso(),
        "relative_path": rel.as_posix(),
    }
    return rel, meta


def resolve_optimize_archive_destination(archive_root: Path, config_name: str, config: dict) -> tuple[Path, dict, bool]:
    """Return a collision-safe destination for an optimize config archive export."""
    rel, meta = derive_optimize_archive_relative_path(config_name, config)
    dest = _validate_archive_path(archive_root / rel, archive_root)
    skipped = False
    if dest.exists():
        existing = _read_json_object_nofollow(dest, archive_root)
        if existing is not None and json_fingerprint(existing) == meta["fingerprint"]:
            skipped = True
        else:
            base = dest.with_name(f"{dest.stem}__{safe_path_part(meta['fingerprint'], 'copy')}.json")
            index = 2
            while base.exists():
                candidate_data = _read_json_object_nofollow(base, archive_root)
                if candidate_data is not None and json_fingerprint(candidate_data) == meta["fingerprint"]:
                    skipped = True
                    break
                base = dest.with_name(f"{dest.stem}__{safe_path_part(meta['fingerprint'], 'copy')}_{index}.json")
                index += 1
            dest = base
            meta["relative_path"] = dest.relative_to(_absolute_path(archive_root)).as_posix()
    return dest, meta, skipped


def _infer_archive_root(path: Path) -> Path:
    """Infer an archive root from a generated path, with a safe parent fallback."""
    absolute_path = _absolute_path(path)
    for candidate in absolute_path.parents:
        try:
            absolute_path.relative_to(candidate / ARCHIVE_LAYOUT_ROOT)
            return candidate
        except ValueError:
            continue
    return absolute_path.parent


def write_optimize_meta(meta_path: Path, meta: dict, archive_root: Path | None = None) -> None:
    """Write sidecar metadata for an archived optimize config."""
    selected_root = _absolute_path(archive_root) if archive_root is not None else _infer_archive_root(meta_path)
    with archive_transaction(selected_root):
        _atomic_write_archive_json(meta_path, meta, selected_root)


def list_archive_optimize_configs(archive_root: Path) -> list[dict]:
    """List archived optimize config JSON files from the versioned archive layout."""
    items = []
    try:
        archive_root = _validate_archive_path(archive_root, archive_root)
        base = _validate_archive_path(archive_root / ARCHIVE_LAYOUT_ROOT, archive_root)
    except RuntimeError:
        return items
    if not base.exists():
        return items
    for config_file in sorted(base.glob("*/optimize/*.json")):
        if config_file.name.endswith(".meta.json"):
            continue
        try:
            config_file = _validate_archive_path(config_file, archive_root, require_exists=True)
            config = _read_json_object_nofollow(config_file, archive_root, required=True)
            meta_file = config_file.with_name(config_file.stem + ".meta.json")
            meta = _read_json_object_nofollow(meta_file, archive_root) or {}
            version = config_version_info(config)
            rel = config_file.relative_to(archive_root).as_posix()
            items.append({
                "name": str(meta.get("name") or config_file.stem),
                "path": str(config_file),
                "relative_path": rel,
                "config_version": str(meta.get("config_version") or meta.get("pb7_config_version") or version["config_version"]),
                "config_family": str(meta.get("config_family") or version["config_family"]),
                "optimize_version": str(meta.get("optimize_version") or version["optimize_version"]),
                "pb7_config_version": str(meta.get("pb7_config_version") or meta.get("config_version") or version["pb7_config_version"]),
                "pbgui_version": str(meta.get("pbgui_version") or version["pbgui_version"]),
                "fingerprint": str(meta.get("fingerprint") or json_fingerprint(config)),
                "created_at": str(meta.get("created_at") or ""),
                "modified": datetime.datetime.fromtimestamp(config_file.stat().st_mtime).isoformat(),
                "meta": meta,
            })
        except (OSError, RuntimeError, ValueError):
            continue
    return items


def archive_item_counts(archive_root: Path, manifest: dict | None = None) -> dict:
    """Return archive item counts from a valid manifest or a safe read-only scan."""
    selected = manifest
    if not (
        isinstance(selected, dict)
        and selected.get("schema_version") == 1
        and isinstance(selected.get("items"), list)
        and all(isinstance(item, dict) for item in selected["items"])
    ):
        selected = load_archive_manifest(archive_root)
    if selected is not None:
        backtests = sum(item.get("type") == "backtest_result" for item in selected["items"])
        optimize = sum(item.get("type") == "optimize_config" for item in selected["items"])
        source = "manifest"
    else:
        backtests = len(list_archive_backtest_results(archive_root))
        optimize = len(list_archive_optimize_configs(archive_root))
        source = "scan"
    return {
        "configs": backtests,
        "results": backtests,
        "optimize_configs": optimize,
        "items": backtests + optimize,
        "source": source,
    }


def build_archive_manifest(archive_root: Path, scored_results: list[dict] | None = None) -> dict:
    """Build a manifest from the current archive contents using scan fallback data."""
    items = []
    results = scored_results if scored_results is not None else score_archive_results(list_archive_backtest_results(archive_root))
    for result in results:
        result_path = Path(str(result.get("path") or ""))
        items.append({
            "type": "backtest_result",
            "name": result.get("config_name", ""),
            "config_version": result.get("config_version", result.get("pb7_config_version", "")),
            "config_family": result.get("config_family", "pb7"),
            "backtest_version": result.get("backtest_version", "v7"),
            "pb7_config_version": result.get("pb7_config_version", ""),
            "pbgui_version": result.get("pbgui_version", ""),
            "path": result.get("display_name", ""),
            "fingerprint": directory_fingerprint(result_path) if result_path.exists() else "",
            "modified": result.get("modified", ""),
            "result_name": result.get("result_name", ""),
            "exchange_dir": result.get("exchange_dir", ""),
            "score": result.get("pbgui_score", {}),
        })
    for config in list_archive_optimize_configs(archive_root):
        items.append({
            "type": "optimize_config",
            "name": config.get("name", ""),
            "config_version": config.get("config_version", config.get("pb7_config_version", "")),
            "config_family": config.get("config_family", "pb7"),
            "optimize_version": config.get("optimize_version", "v7"),
            "pb7_config_version": config.get("pb7_config_version", ""),
            "pbgui_version": config.get("pbgui_version", ""),
            "path": config.get("relative_path", ""),
            "fingerprint": config.get("fingerprint", ""),
            "created_at": config.get("created_at", ""),
            "modified": config.get("modified", ""),
        })
    items.sort(key=lambda item: (str(item.get("type") or ""), str(item.get("path") or "")))
    return {"schema_version": 1, "generated_at": utc_now_iso(), "items": items}


def rebuild_archive_manifest(archive_root: Path) -> dict:
    """Rebuild and atomically write the archive manifest."""
    with archive_transaction(archive_root):
        manifest = build_archive_manifest(archive_root)
        _atomic_write_archive_json(archive_manifest_path(archive_root), manifest, archive_root)
        return manifest


def update_archive_scores_and_readme(archive_root: Path) -> dict:
    """Recalculate scores, write manifest, README score summary, and full score page."""
    with archive_transaction(archive_root):
        scored = score_archive_results(list_archive_backtest_results(archive_root))
        manifest = build_archive_manifest(archive_root, scored_results=scored)
        _atomic_write_archive_json(archive_manifest_path(archive_root), manifest, archive_root)
        markdown = build_archive_scores_markdown(scored)
        scores_page_markdown = build_archive_scores_page_markdown(scored, archive_root)
        scores_page_html = build_archive_scores_html(scored, archive_root)
        _atomic_write_archive_text(archive_scores_path(archive_root), scores_page_markdown, archive_root)
        _atomic_write_archive_text(archive_scores_html_path(archive_root), scores_page_html, archive_root)
        readme_markdown = update_archive_readme(archive_root, scores_markdown=build_archive_scores_summary_markdown(scored, archive_root))
        return {
            "ok": True,
            "score_version": ARCHIVE_SCORE_VERSION,
            "generated_at": manifest.get("generated_at", utc_now_iso()),
            "total": len(scored),
            "scored": len([result for result in scored if result.get("pbgui_score")]),
            "rows": archive_score_rows(scored, limit=100),
            "markdown": markdown,
            "readme_markdown": readme_markdown,
            "scores_page_markdown": scores_page_markdown,
            "scores_page_html": scores_page_html,
            "scores_path": ARCHIVE_SCORES.as_posix(),
            "scores_html_path": ARCHIVE_SCORES_HTML.as_posix(),
            "manifest": {"schema_version": manifest.get("schema_version"), "items": len(manifest.get("items", []))},
        }


def archive_config_group_dir(result_dir: Path, archive_root: Path) -> Path:
    """Return the archive config group directory for a result path."""
    rel_parts = result_dir.resolve().relative_to(archive_root.resolve()).parts
    if len(rel_parts) >= 6 and rel_parts[0] == "pbgui" and rel_parts[1] == "configs" and rel_parts[3] == "backtests":
        return archive_root / Path(*rel_parts[:5])
    if len(rel_parts) >= 3:
        return result_dir.parent.parent
    return result_dir.parent


def _remove_liquidated_results_locked(archive_root: Path, paths: list[str], scope: str, dry_run: bool) -> dict:
    """Remove liquidated results while the archive transaction lock is held."""
    result_paths = []
    rejected = []
    seen_paths = set()
    for raw_path in paths or []:
        try:
            result_dir = _validate_archive_path(Path(str(raw_path)), archive_root, require_exists=True)
            _validate_directory_tree_no_symlinks(result_dir)
            _read_json_object_nofollow(result_dir / "analysis.json", archive_root, required=True)
            _read_json_object_nofollow(result_dir / "config.json", archive_root, required=True)
        except (OSError, RuntimeError, ValueError) as exc:
            rejected.append({"path": str(raw_path), "ok": False, "removed": False, "outcome": "rejected", "error": str(exc)})
            continue
        text_path = str(result_dir)
        if text_path in seen_paths:
            continue
        seen_paths.add(text_path)
        result_paths.append(result_dir)
    scope = scope or "selected_results"
    items = list(rejected)
    removed = 0
    failed = len(rejected)

    def remove_path(target: Path, item: dict) -> None:
        """Remove one validated directory and record a verified outcome."""
        nonlocal removed, failed
        item["removed"] = False
        item["ok"] = True
        if dry_run:
            item["outcome"] = "dry_run"
            return
        try:
            _validate_archive_path(target, archive_root, require_exists=True)
            _validate_directory_tree_no_symlinks(target)
            shutil.rmtree(str(target))
            if target.exists() or target.is_symlink():
                raise RuntimeError(f"Archive path still exists after removal: {target}")
            cleanup_empty_parents(target, archive_root)
        except Exception as exc:
            item["ok"] = False
            item["outcome"] = "failed"
            item["error"] = str(exc)
            failed += 1
            return
        item["removed"] = True
        item["outcome"] = "removed"
        removed += 1

    if scope == "config_if_all_results_liquidated":
        groups = sorted({archive_config_group_dir(path, archive_root) for path in result_paths})
        for group in groups:
            summaries = []
            valid_group = True
            try:
                _validate_archive_path(group, archive_root, require_exists=True)
                _validate_directory_tree_no_symlinks(group)
                for analysis_file in sorted(group.glob("**/analysis.json")):
                    result_dir = _validate_archive_path(analysis_file.parent, archive_root, require_exists=True)
                    summaries.append(summarize_backtest_result(result_dir, archive_root))
            except Exception as exc:
                items.append({"path": str(group), "config_name": group.name, "reason": "unsafe_or_invalid_group", "ok": False, "removed": False, "outcome": "rejected", "error": str(exc)})
                failed += 1
                valid_group = False
            if not valid_group:
                continue
            if not summaries or not all(item.get("liquidated") for item in summaries):
                continue
            item = {"path": str(group), "config_name": group.name, "reason": "all_results_liquidated", "results": len(summaries)}
            items.append(item)
            remove_path(group, item)
        matched = sum(item.get("reason") == "all_results_liquidated" for item in items)
        return {"ok": failed == 0, "dry_run": dry_run, "matched": matched, "removed": removed, "failed": failed, "items": items}
    for result_dir in result_paths:
        try:
            summary = summarize_backtest_result(result_dir, archive_root)
        except Exception as exc:
            items.append({"path": str(result_dir), "ok": False, "removed": False, "outcome": "rejected", "error": str(exc)})
            failed += 1
            continue
        if not summary.get("liquidated"):
            continue
        item = {"path": str(result_dir), "config_name": summary.get("config_name", ""), "reason": summary.get("liquidation_reason", "liquidated")}
        items.append(item)
        remove_path(result_dir, item)
    matched = sum(bool(item.get("reason")) and item.get("reason") != "unsafe_or_invalid_group" for item in items)
    return {"ok": failed == 0, "dry_run": dry_run, "matched": matched, "removed": removed, "failed": failed, "items": items}


def remove_liquidated_results(archive_root: Path, paths: list[str], scope: str, dry_run: bool) -> dict:
    """Remove or preview liquidated archived result directories with safety checks."""
    with archive_transaction(archive_root):
        return _remove_liquidated_results_locked(archive_root, paths, scope, dry_run)


def _archive_duplicate_key(result_dir: Path, archive_root: Path) -> tuple | None:
    """Return a conservative duplicate key for an archived result."""
    _validate_archive_path(result_dir, archive_root, require_exists=True)
    _validate_directory_tree_no_symlinks(result_dir)
    _read_json_object_nofollow(result_dir / "analysis.json", archive_root, required=True)
    _read_json_object_nofollow(result_dir / "config.json", archive_root, required=True)
    summary = summarize_backtest_result(result_dir, archive_root)

    def rounded(value: Any, digits: int) -> float | str:
        try:
            return round(float(value or 0), digits)
        except (TypeError, ValueError):
            return str(value or "")

    return (
        summary.get("config_version", summary.get("pb7_config_version", "")),
        summary.get("backtest_version", "v7"),
        summary.get("config_name", ""),
        summary.get("exchange_dir", ""),
        tuple(summary.get("exchanges") or []),
        summary.get("start_date", ""),
        summary.get("end_date", ""),
        rounded(summary.get("starting_balance"), 8),
        rounded(summary.get("final_balance"), 8),
        rounded(summary.get("adg"), 8),
        rounded(summary.get("gain"), 8),
        rounded(summary.get("drawdown_worst"), 8),
        rounded(summary.get("sharpe_ratio"), 8),
        rounded(summary.get("equity_balance_diff_neg_max"), 8),
        rounded(summary.get("btc_collateral_cap"), 8),
        rounded(summary.get("twe_long"), 8),
        rounded(summary.get("twe_short"), 8),
        rounded(summary.get("pos_long"), 8),
        rounded(summary.get("pos_short"), 8),
    )


def _remove_duplicate_results_locked(archive_root: Path, paths: list[str], scope: str, dry_run: bool) -> dict:
    """Remove duplicate results while the archive transaction lock is held."""
    result_paths = []
    items = []
    seen_paths: set[str] = set()
    failed = 0
    for raw_path in paths or []:
        lexical_path = str(_absolute_path(Path(str(raw_path))))
        if lexical_path in seen_paths:
            continue
        seen_paths.add(lexical_path)
        try:
            result_dir = _validate_archive_path(Path(str(raw_path)), archive_root, require_exists=True)
            _validate_directory_tree_no_symlinks(result_dir)
            _read_json_object_nofollow(result_dir / "analysis.json", archive_root, required=True)
            _read_json_object_nofollow(result_dir / "config.json", archive_root, required=True)
        except (OSError, RuntimeError, ValueError) as exc:
            items.append({"path": str(raw_path), "ok": False, "removed": False, "outcome": "rejected", "error": str(exc)})
            failed += 1
            continue
        result_paths.append(result_dir)

    groups: dict[tuple, list[Path]] = {}
    for result_dir in result_paths:
        try:
            key = _archive_duplicate_key(result_dir, archive_root)
        except (OSError, RuntimeError, ValueError) as exc:
            items.append({"path": str(result_dir), "ok": False, "removed": False, "outcome": "rejected", "error": str(exc)})
            failed += 1
            continue
        groups.setdefault(key, []).append(result_dir)

    removed = 0
    for group_paths in groups.values():
        if len(group_paths) < 2:
            continue
        try:
            ordered = sorted(
                group_paths,
                key=lambda path: ((path / "analysis.json").stat(follow_symlinks=False).st_mtime, path.name),
                reverse=True,
            )
            keep = ordered[0]
            keep_summary = summarize_backtest_result(keep, archive_root)
        except (OSError, RuntimeError, ValueError) as exc:
            for result_dir in group_paths:
                items.append({"path": str(result_dir), "ok": False, "removed": False, "outcome": "failed", "error": str(exc)})
                failed += 1
            continue
        for duplicate in ordered[1:]:
            try:
                summary = summarize_backtest_result(duplicate, archive_root)
            except (OSError, RuntimeError, ValueError) as exc:
                items.append({"path": str(duplicate), "keep_path": str(keep), "ok": False, "removed": False, "outcome": "rejected", "error": str(exc)})
                failed += 1
                continue
            item = {
                "path": str(duplicate),
                "keep_path": str(keep),
                "config_name": summary.get("config_name", ""),
                "result_name": summary.get("result_name", ""),
                "keep_result_name": keep_summary.get("result_name", ""),
                "reason": "duplicate_of_newer_result",
                "ok": True,
                "removed": False,
                "outcome": "dry_run" if dry_run else "pending",
            }
            items.append(item)
            if dry_run:
                continue
            try:
                _validate_archive_path(duplicate, archive_root, require_exists=True)
                _validate_directory_tree_no_symlinks(duplicate)
                _read_json_object_nofollow(duplicate / "analysis.json", archive_root, required=True)
                _read_json_object_nofollow(duplicate / "config.json", archive_root, required=True)
                shutil.rmtree(str(duplicate))
                if duplicate.exists() or duplicate.is_symlink():
                    raise RuntimeError(f"Archive path still exists after removal: {duplicate}")
                cleanup_empty_parents(duplicate, archive_root)
            except Exception as exc:
                item["ok"] = False
                item["outcome"] = "failed"
                item["error"] = str(exc)
                failed += 1
                continue
            item["removed"] = True
            item["outcome"] = "removed"
            removed += 1

    scope = scope or "selected_results"
    matched = sum(item.get("reason") == "duplicate_of_newer_result" for item in items)
    return {"ok": failed == 0, "dry_run": dry_run, "scope": scope, "matched": matched, "removed": removed, "failed": failed, "items": items}


def remove_duplicate_results(archive_root: Path, paths: list[str], scope: str, dry_run: bool) -> dict:
    """Remove or preview duplicate archived results, keeping the newest result in each duplicate group."""
    with archive_transaction(archive_root):
        return _remove_duplicate_results_locked(archive_root, paths, scope, dry_run)


def ensure_config_version(config: dict, template_loader: Callable[[], dict]) -> dict:
    """Preserve or inject the current PB7 config_version into an optimize config."""
    if not isinstance(config, dict):
        return config
    if str(config.get("config_version") or "").strip():
        return config
    try:
        template = template_loader()
    except Exception:
        template = {}
    version = str((template or {}).get("config_version") or "").strip()
    if version:
        config["config_version"] = version
    return config
