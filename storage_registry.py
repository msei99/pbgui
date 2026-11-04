"""Storage categories, filesystem scanning and cleanup primitives.

The registry is shared by the API controller (preview, summaries) and the
storage worker subprocess (scan, cleanup). It never deletes outside the
approved root of a category and always re-lists items from disk before a
cleanup, so a plan can only remove items that still match its selection.
"""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory

SERVICE = "Storage"

DAY = 86400
ACTIVE_GRACE_S = DAY
TREE_DEPTH = 4
TREE_CHILDREN = 30
TREE_MIN_BYTES = 1024 * 1024
TRASH_PREFIX = ".pbgui-storage-trash-"
SNAPSHOT_VERSION = 1

TIERS = ("safe", "optional", "review", "protected")
GROUPS = {
    "logs": "Logs",
    "results": "Jobs & Results",
    "caches": "Caches & Market Data",
    "backups": "Backups & Leftovers",
}

_ROTATED_RE = re.compile(r"^(?P<base>.+\.log)\.\d+$")
_DB_COMPANION_RE = re.compile(r"-(?:wal|shm|journal)$")
_COPY_RE = re.compile(r"(?:\s\((?:Kopie|Kopieren|Copy|copy)(?:\s\d+)?\)|\s(?:copy|Kopie)(?:\s\d+)?)$")
_LEFTOVER_CACHE_RE = re.compile(r"(?:_old|_org|_orig|_bak|_backup|\.bak)$")
_VAST_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_PB_VENV_RE = re.compile(r"^\.?venv[_-]?(?:pb|passivbot)", re.IGNORECASE)
_RUN_LOG_DIRS = ("optimizes", "optimizes_v8", "backtests", "backtests_v8")


class ScanCancelled(Exception):
    """Raised when a stop was requested while walking the filesystem."""


# ── Paths and context ─────────────────────────────────────────


def _venv_root(python_path: str | None) -> Path | None:
    """Return the virtualenv directory that owns a configured interpreter."""
    if not python_path:
        return None
    candidate = Path(python_path).expanduser().parent.parent
    return candidate if (candidate / "pyvenv.cfg").is_file() else None


def _real(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        return os.path.realpath(path)
    except OSError:
        return str(path)


@dataclass
class StorageContext:
    """Resolved locations plus the optional directory statistics of a scan."""

    pbgui: Path
    pb7: Path | None = None
    pb8: Path | None = None
    pb7_python: str = ""
    pb8_python: str = ""
    now: float = field(default_factory=time.time)
    dirstats: dict[str, tuple[int, int, float]] = field(default_factory=dict)
    fresh_stats: bool = False
    only_ids: set[str] | None = None
    stop: Callable[[], bool] | None = None

    @classmethod
    def from_settings(cls, pbgui: Path | None = None, **kwargs: Any) -> "StorageContext":
        """Build a context from pbgui.ini path settings."""
        from pbgui_purefunc import PBGDIR, load_ini

        def _dir(key: str) -> Path | None:
            value = str(load_ini("main", key) or "").strip()
            if not value:
                return None
            path = Path(value).expanduser()
            return path if path.is_dir() else None

        return cls(
            pbgui=Path(pbgui or PBGDIR),
            pb7=_dir("pb7dir"),
            pb8=_dir("pb8dir"),
            pb7_python=str(load_ini("main", "pb7venv") or "").strip(),
            pb8_python=str(load_ini("main", "pb8venv") or "").strip(),
            **kwargs,
        )

    @property
    def data(self) -> Path:
        return self.pbgui / "data"

    def check_stop(self) -> None:
        if self.stop is not None and self.stop():
            raise ScanCancelled()

    def wants_deep(self, item_id: str) -> bool:
        """Return whether an item needs fresh recursive statistics."""
        return self.fresh_stats and (self.only_ids is None or item_id in self.only_ids)

    def stats(self, path: Path, deep: bool | None = None) -> tuple[int, int, float]:
        """Return (bytes, files, newest mtime) for a file or directory."""
        try:
            st = os.lstat(path)
        except OSError:
            return 0, 0, 0.0
        if not stat.S_ISDIR(st.st_mode):
            return allocated_bytes(st), 1, st.st_mtime
        cached = self.dirstats.get(str(path))
        if cached is not None:
            return cached[0], cached[1], max(cached[2], st.st_mtime)
        if self.fresh_stats if deep is None else deep:
            size, files, mtime = walk_stats(str(path), stop=self.stop)
            return size, files, max(mtime, st.st_mtime)
        return allocated_bytes(st), 0, st.st_mtime


def allocated_bytes(st: os.stat_result) -> int:
    """Return on-disk usage like du, falling back to the apparent size."""
    blocks = getattr(st, "st_blocks", None)
    return int(blocks) * 512 if blocks is not None else int(st.st_size)


def walk_stats(path: str, *, stop: Callable[[], bool] | None = None,
               seen: set[tuple[int, int]] | None = None,
               dirstats: dict[str, tuple[int, int, float]] | None = None,
               children: dict[str, list[str]] | None = None,
               skip: set[str] | None = None, depth: int = 0) -> tuple[int, int, float]:
    """Sum allocated bytes, file count and newest mtime below a directory.

    Symlinks are counted as links and never followed. Hard links are counted
    once per scan. When ``dirstats`` is given, every visited directory is
    recorded; ``children`` records subdirectories up to ``TREE_DEPTH``.
    """
    seen = seen if seen is not None else set()
    size = files = 0
    newest = 0.0
    try:
        iterator = os.scandir(path)
    except OSError:
        return 0, 0, 0.0
    subdirs: list[str] = []
    with iterator:
        for entry in iterator:
            if stop is not None and stop():
                raise ScanCancelled()
            try:
                st = entry.stat(follow_symlinks=False)
            except OSError:
                continue
            if stat.S_ISDIR(st.st_mode):
                if skip and entry.path in skip:
                    continue
                sub_size, sub_files, sub_mtime = walk_stats(
                    entry.path, stop=stop, seen=seen, dirstats=dirstats, children=children,
                    skip=skip, depth=depth + 1,
                )
                size += sub_size + allocated_bytes(st)
                files += sub_files
                newest = max(newest, sub_mtime, st.st_mtime)
                if children is not None and depth < TREE_DEPTH:
                    subdirs.append(entry.path)
                continue
            if st.st_nlink > 1:
                key = (st.st_dev, st.st_ino)
                if key in seen:
                    continue
                seen.add(key)
            size += allocated_bytes(st)
            files += 1
            newest = max(newest, st.st_mtime)
    if dirstats is not None:
        dirstats[path] = (size, files, newest)
    if children is not None and depth < TREE_DEPTH:
        children[path] = subdirs
    return size, files, newest


# ── Items and categories ──────────────────────────────────────


def _entries(directory: Path, *, dirs: bool = True, files: bool = True) -> list[os.DirEntry]:
    """List direct children, skipping storage trash and lock files."""
    try:
        with os.scandir(directory) as iterator:
            result = []
            for entry in iterator:
                if entry.name.startswith(TRASH_PREFIX) or entry.name.endswith(".lock"):
                    continue
                try:
                    is_dir = entry.is_dir(follow_symlinks=False)
                except OSError:
                    continue
                if (is_dir and dirs) or (not is_dir and files):
                    result.append(entry)
            return result
    except OSError:
        return []


def make_item(ctx: StorageContext, item_id: str, paths: list[Path], *, label: str | None = None,
              blocked: str | None = None, meta: dict[str, Any] | None = None,
              sort_mtime: float | None = None) -> dict[str, Any]:
    """Build a JSON-serializable cleanup item from one or more paths."""
    size = files = 0
    newest = 0.0
    path_sizes = {}
    deep = ctx.wants_deep(item_id)
    for path in paths:
        ctx.check_stop()
        path_size, path_files, path_mtime = ctx.stats(path, deep=deep)
        path_sizes[str(path)] = path_size
        size += path_size
        files += path_files
        newest = max(newest, path_mtime)
    if sort_mtime is None:
        sort_mtime = newest
    item = {
        "id": item_id,
        "label": label or item_id,
        "paths": [str(path) for path in paths],
        "size": size,
        "files": files,
        "mtime": newest,
        "sort_mtime": sort_mtime,
        "blocked": blocked,
        "meta": meta or {},
    }
    if len(paths) > 1:
        item["path_sizes"] = path_sizes
    return item


@dataclass
class Listing:
    """Items of one category plus an optional category-wide blocker."""

    items: list[dict[str, Any]] = field(default_factory=list)
    blocked: str | None = None
    available: bool = True


@dataclass(frozen=True)
class Category:
    """One cleanup or display category of the Storage page."""

    id: str
    group: str
    tier: str
    label: str
    tip: str
    lister: Callable[[StorageContext], Listing]
    roots: Callable[[StorageContext], list[Path]]
    deleter: str = "paths"
    default_days: int | None = None
    default_keep: int | None = None
    auto_allowed: bool = False
    # Blocker that can appear without touching the item's files (a job that
    # was queued, a process that started); checked right before each deletion.
    runtime_check: Callable[[StorageContext, dict[str, Any]], str | None] | None = None

    def public(self) -> dict[str, Any]:
        """Return the static description sent to the browser."""
        return {
            "id": self.id,
            "group": self.group,
            "tier": self.tier,
            "label": self.label,
            "tip": self.tip,
            "default_days": self.default_days,
            "default_keep": self.default_keep,
            "auto_allowed": self.auto_allowed,
            "deletable": self.tier != "protected",
        }


def _active_block(ctx: StorageContext, mtime: float) -> str | None:
    return "Changed within the last 24 hours" if mtime and ctx.now - mtime < ACTIVE_GRACE_S else None


def _children_items(ctx: StorageContext, directory: Path | None, *, dirs: bool = True, files: bool = True,
                    depth: int = 1, guard_active: bool = False, id_prefix: str = "",
                    name_filter: Callable[[str], bool] | None = None) -> Listing:
    """List children (optionally grandchildren) of a directory as items."""
    if directory is None or not directory.is_dir():
        return Listing(available=False)
    items = []
    level = [directory]
    for _ in range(depth - 1):
        level = [Path(entry.path) for parent in level for entry in _entries(parent, files=False)]
    for parent in level:
        for entry in _entries(parent, dirs=dirs, files=files):
            if name_filter is not None and not name_filter(entry.name):
                continue
            path = Path(entry.path)
            item = make_item(ctx, id_prefix + str(path.relative_to(directory)), [path])
            if guard_active and not item["blocked"]:
                item["blocked"] = _active_block(ctx, item["mtime"])
            items.append(item)
    return Listing(items=items)


# Logs


def _list_rotated_logs(ctx: StorageContext) -> Listing:
    root = ctx.data / "logs"
    if not root.is_dir():
        return Listing(available=False)
    items = []
    for dirpath, dirnames, filenames in os.walk(root):
        ctx.check_stop()
        dirnames[:] = [name for name in dirnames if not name.startswith(TRASH_PREFIX)]
        for name in filenames:
            match = _ROTATED_RE.match(name)
            if not match:
                continue
            path = Path(dirpath) / name
            items.append(make_item(ctx, str(path.relative_to(root)), [path],
                                   meta={"base": str(Path(dirpath) / match.group("base"))}))
    return Listing(items=items)


def _active_task_job_ids() -> set[str]:
    try:
        from task_queue import get_task_state_dir
    except Exception:
        return set()
    active = set()
    for state in ("pending", "running"):
        try:
            active.update(path.stem for path in get_task_state_dir(state).glob("*.json"))
        except OSError:
            continue
    return active


_JOB_ACTIVE = "Job is pending or running"


def _job_log_in_use(ctx: StorageContext, item: dict[str, Any]) -> str | None:  # noqa: ARG001
    return _JOB_ACTIVE if str(item["id"]).removesuffix(".log") in _active_task_job_ids() else None


def _list_job_logs(ctx: StorageContext) -> Listing:
    root = ctx.data / "logs" / "jobs"
    if not root.is_dir():
        return Listing(available=False)
    active = _active_task_job_ids()
    items = []
    for entry in _entries(root, dirs=False):
        if not entry.name.endswith(".log"):
            continue
        blocked = _JOB_ACTIVE if entry.name[:-4] in active else None
        items.append(make_item(ctx, entry.name, [Path(entry.path)], blocked=blocked))
    return Listing(items=items)


def _list_run_logs(ctx: StorageContext) -> Listing:
    root = ctx.data / "logs"
    items = []
    available = False
    for name in _RUN_LOG_DIRS:
        listing = _children_items(ctx, root / name, dirs=False, guard_active=True, id_prefix=f"{name}/")
        available = available or listing.available
        items.extend(listing.items)
    return Listing(items=items, available=available)


# Jobs and results


def _vast_components(ctx: StorageContext) -> tuple[list[tuple[list[dict], bool]], str | None]:
    """Group cloud jobs into retry lineages and mark removable ones."""
    root = ctx.data / "vast"
    if not (root / "jobs").is_dir():
        return [], None
    try:
        from vast_jobs import JobStore
        from vast_queue import CloudQueue, can_remove_job
    except Exception as exc:  # pragma: no cover - import failures are environment problems
        return [], f"Cloud queue unavailable: {type(exc).__name__}"
    try:
        store = JobStore(root)
        queue = CloudQueue(store)
        rows = [row for row in store.list() if row.get("kind") != "worker" and not row.get("deleted_at")]
    except Exception as exc:
        return [], f"Cloud queue cannot be read: {type(exc).__name__}"
    by_id = {row["id"]: row for row in rows}
    parent: dict[str, str] = {row_id: row_id for row_id in by_id}

    def _find(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    for row in rows:
        source = row.get("requeue_from")
        if source in by_id:
            parent[_find(row["id"])] = _find(source)
    groups: dict[str, list[dict]] = {}
    for row_id, row in by_id.items():
        groups.setdefault(_find(row_id), []).append(row)
    components = []
    for members in groups.values():
        removable = True
        for row in members:
            try:
                worker = queue.worker_for(row.get("lease_id"))
            except Exception:
                worker = None
            if not can_remove_job(row, worker):
                removable = False
        members.sort(key=lambda row: float(row.get("created_at") or 0))
        components.append((members, removable))
    return components, None


def _vast_listing(ctx: StorageContext, *, completed: bool) -> Listing:
    components, error = _vast_components(ctx)
    if error:
        return Listing(blocked=error)
    if not (ctx.data / "vast" / "jobs").is_dir():
        return Listing(available=False)
    items = []
    for members, removable in components:
        statuses = {str(row.get("status")) for row in members}
        if ("completed" in statuses) != completed:
            continue
        first = members[0]
        paths = [ctx.data / "vast" / "jobs" / row["id"] for row in members]
        finished = max(float(row.get("finished_at") or row.get("updated_at") or 0) for row in members)
        name = str(first.get("config_name") or "Cloud job")
        label = f"{name} · {', '.join(sorted(statuses))}"
        if len(members) > 1:
            label += f" · {len(members)} attempts"
        item = make_item(ctx, first["id"], paths, label=label,
                         blocked=None if removable else "Active or unfinished cloud job",
                         meta={"job_ids": [row["id"] for row in members], "status": sorted(statuses)},
                         sort_mtime=finished)
        if finished:
            item["mtime"] = finished  # job state, not lock-file churn inside the folder
        items.append(item)
    return Listing(items=items)


def _list_vast_inactive(ctx: StorageContext) -> Listing:
    return _vast_listing(ctx, completed=False)


def _list_vast_completed(ctx: StorageContext) -> Listing:
    return _vast_listing(ctx, completed=True)


def _sub(base: Path | None, *parts: str) -> Path | None:
    return base.joinpath(*parts) if base is not None else None


# Caches


def _pb_running(ctx: StorageContext, base: Path | None, scripts: tuple[str, ...]) -> bool:
    """Return whether a Passivbot backtest/optimize process uses ``base``."""
    if base is None:
        return False
    try:
        import psutil
    except ImportError:  # pragma: no cover
        return False
    base_real = _real(base)
    for proc in psutil.process_iter(["cmdline", "cwd"]):
        joined = " ".join(proc.info.get("cmdline") or [])
        if not any(script in joined for script in scripts):
            continue
        cwd = _real(Path(proc.info["cwd"])) if proc.info.get("cwd") else ""
        if base_real in joined or cwd == base_real or cwd.startswith(base_real + os.sep):
            return True
    return False


def _pb7_backtest_running(ctx: StorageContext, item: dict[str, Any] | None = None) -> str | None:  # noqa: ARG001
    if _pb_running(ctx, ctx.pb7, ("backtest.py", "optimize.py")):
        return "A PB7 backtest or optimize is running"
    return None


def _list_pb7_hlcvs(ctx: StorageContext) -> Listing:
    listing = _children_items(ctx, _sub(ctx.pb7, "caches", "hlcvs_data"), files=False)
    if listing.available:
        listing.blocked = _pb7_backtest_running(ctx)
    return listing


def _list_pb7_legacy(ctx: StorageContext) -> Listing:
    listing = _children_items(ctx, _sub(ctx.pb7, "historical_data"))
    if listing.available and not (ctx.pb7 / "caches" / "ohlcv" / ".migration_done").is_file():
        listing.blocked = "PB7 has not finished migrating its legacy data"
    return listing


def _list_pb7_cache_leftovers(ctx: StorageContext) -> Listing:
    return _children_items(ctx, _sub(ctx.pb7, "caches", "ohlcv"), files=False,
                           name_filter=lambda name: bool(_LEFTOVER_CACHE_RE.search(name)))


def _protected(ctx: StorageContext, entries: list[tuple[str, Path | None]]) -> Listing:
    items = []
    for label, path in entries:
        if path is not None and (path.exists() or path.is_symlink()):
            items.append(make_item(ctx, label, [path], label=label, blocked="Protected"))
    return Listing(items=items, available=bool(items))


def _list_protected_market(ctx: StorageContext) -> Listing:
    return _protected(ctx, [
        ("PBGui market data archive", ctx.data / "ohlcv"),
        ("PB7 OHLCV cache", _sub(ctx.pb7, "caches", "ohlcv")),
        ("PB7 OHLCV store", _sub(ctx.pb7, "caches", "ohlcvs")),
    ])


# Backups and leftovers


def _list_db_backups(ctx: StorageContext) -> Listing:
    return _children_items(ctx, ctx.data / "backup" / "db", dirs=False,
                           name_filter=lambda name: name.startswith("pbgui") and name.endswith(".db"))


def _list_db_tools_backups(ctx: StorageContext) -> Listing:
    root = ctx.data / "backup" / "db-tools"
    if not root.is_dir():
        return Listing(available=False)
    grouped: dict[str, list[Path]] = {}
    for entry in _entries(root):
        grouped.setdefault(_DB_COMPANION_RE.sub("", entry.name), []).append(Path(entry.path))
    items = []
    for key, paths in sorted(grouped.items()):
        item = make_item(ctx, key, sorted(paths))
        item["blocked"] = _active_block(ctx, item["mtime"])
        items.append(item)
    return Listing(items=items)


def _list_db_repair(ctx: StorageContext) -> Listing:
    return _children_items(ctx, ctx.data / "backup", files=False, guard_active=True,
                           name_filter=lambda name: name.startswith("db-repair-"))


def _list_release_worktrees(ctx: StorageContext) -> Listing:
    return _children_items(ctx, ctx.data / "release_worktrees", files=False)


def _list_manual_copies(ctx: StorageContext) -> Listing:
    return _children_items(ctx, ctx.data, name_filter=lambda name: bool(_COPY_RE.search(name)))


def venv_usage(ctx: StorageContext) -> dict[str, str]:
    """Return real venv paths that are in use, mapped to the reason."""
    used: dict[str, str] = {}
    current = _real(Path(sys.prefix))
    if current:
        used[current] = "Used by PBGui"
    for python, reason in ((ctx.pb7_python, "Configured for PB7"), (ctx.pb8_python, "Configured for PB8")):
        root = _venv_root(python)
        if root is not None:
            used.setdefault(_real(root), reason)
    try:
        import psutil

        roots = [_real(path) for path in _venv_candidates(ctx)]
        for proc in psutil.process_iter(["exe", "cmdline", "cwd"]):
            for root in _process_venvs(proc, roots):
                used.setdefault(root, "Used by a running process")
    except Exception:
        pass
    units = Path.home() / ".config" / "systemd" / "user"
    try:
        unit_text = "\n".join(path.read_text(encoding="utf-8", errors="replace")
                              for path in units.glob("*.service") if path.is_file())
    except OSError:
        unit_text = ""
    for candidate in _venv_candidates(ctx):
        names = {str(candidate), _real(candidate)}
        names.update(str(link) for link in _venv_links(ctx) if _real(link) == _real(candidate))
        if unit_text and any(name and name + os.sep in unit_text for name in names):
            used.setdefault(_real(candidate), "Used by a systemd service")
    return used


def _interpreter_path(path: str, cwd: str = "") -> str:
    """Resolve the folders of an interpreter path but keep its last symlink.

    A venv's ``bin/python`` is usually a symlink to the system interpreter, so
    resolving it completely would lose the venv the process runs in.
    """
    if not path or (os.sep not in path):
        return ""
    if not os.path.isabs(path):
        if not cwd:
            return ""
        path = os.path.join(cwd, path)
    path = os.path.normpath(path)
    return os.path.join(_real(Path(os.path.dirname(path))), os.path.basename(path))


def _process_venvs(proc: Any, roots: list[str]) -> list[str]:
    """Return the venv roots a process runs from (executable, argv[0] or VIRTUAL_ENV)."""
    info = getattr(proc, "info", {}) or {}
    cmdline = info.get("cmdline") or []
    paths = [_interpreter_path(info.get("exe") or ""),
             _interpreter_path(cmdline[0] if cmdline else "", info.get("cwd") or "")]
    try:
        virtual_env = (proc.environ() or {}).get("VIRTUAL_ENV") or ""
    except Exception:
        virtual_env = ""
    if virtual_env:
        paths.append(os.path.join(_real(Path(virtual_env)), ""))
    return [root for root in roots if any(path.startswith(root + os.sep) for path in paths if path)]


def _venv_in_use(ctx: StorageContext, paths: list[Path]) -> str | None:
    used = venv_usage(ctx)
    for path in paths:
        reason = used.get(_real(path))
        if reason:
            return f"{reason}: {path.name}"
    return None


def _venv_links(ctx: StorageContext) -> list[Path]:
    parent = ctx.pbgui.parent
    return [Path(entry.path) for entry in _entries(parent, files=True) if entry.is_symlink()]


def _venv_candidates(ctx: StorageContext) -> list[Path]:
    """Return PB/PBGui virtualenvs next to PBGui plus the configured ones."""
    found: dict[str, Path] = {}
    for entry in _entries(ctx.pbgui.parent, files=False):
        path = Path(entry.path)
        if _PB_VENV_RE.match(entry.name) and (path / "pyvenv.cfg").is_file():
            found[_real(path)] = path
    for python in (ctx.pb7_python, ctx.pb8_python):
        root = _venv_root(python)
        if root is not None and not root.is_symlink():
            found.setdefault(_real(root), root)
    return sorted(found.values())


def _list_venvs(ctx: StorageContext) -> Listing:
    used = venv_usage(ctx)
    parent = ctx.pbgui.parent
    items = []
    for path in _venv_candidates(ctx):
        if path.parent != parent:
            continue
        items.append(make_item(ctx, path.name, [path], blocked=used.get(_real(path))))
    return Listing(items=items, available=bool(items))


def _list_protected_core(ctx: StorageContext) -> Listing:
    return _protected(ctx, [
        ("PBGui database", ctx.data / "pbgui.db"),
        ("Credentials", ctx.data / "credentials"),
        ("API keys", ctx.data / "api-keys"),
        ("Instances", ctx.data / "instances"),
        ("Run configs v7", ctx.data / "run_v7"),
        ("Run configs v8", ctx.data / "run_v8"),
        ("Configuration backups", ctx.data / "backup" / "api-keys"),
    ])


def _r(*parts: str) -> Callable[[StorageContext], list[Path]]:
    return lambda ctx: [ctx.data.joinpath(*parts)]


def _pb(attr: str, *parts: str) -> Callable[[StorageContext], list[Path]]:
    def _roots(ctx: StorageContext) -> list[Path]:
        base = getattr(ctx, attr)
        return [base.joinpath(*parts)] if base is not None else []
    return _roots


CATEGORIES: tuple[Category, ...] = (
    Category("logs_rotated", "logs", "safe", "Rotated log files",
             "Older generations (*.log.1, *.log.2 …) kept by log rotation. Current logs are not touched.",
             _list_rotated_logs, _r("logs"), deleter="rotated_log", default_days=7, auto_allowed=True),
    Category("logs_jobs", "logs", "safe", "Market data job logs",
             "Per-job logs in data/logs/jobs. Logs of pending or running jobs are kept.",
             _list_job_logs, _r("logs", "jobs"), default_days=30, auto_allowed=True,
             runtime_check=_job_log_in_use),
    Category("logs_runs", "logs", "safe", "Backtest & optimize run logs",
             "Run logs in data/logs/optimizes, optimizes_v8, backtests and backtests_v8.",
             _list_run_logs, lambda ctx: [ctx.data / "logs" / name for name in _RUN_LOG_DIRS],
             default_days=60, auto_allowed=True),
    Category("vast_inactive", "results", "optional", "Cancelled & failed cloud jobs",
             "Vast job histories whose retry lineage contains no completed attempt. Uses the Vast queue's own purge.",
             _list_vast_inactive, _r("vast", "jobs"), deleter="vast", default_days=14),
    Category("vast_completed", "results", "review", "Completed cloud jobs",
             "Vast job histories with a completed attempt, including their inputs and collected results.",
             _list_vast_completed, _r("vast", "jobs"), deleter="vast"),
    Category("pb7_optimize_results", "results", "review", "PB7 optimize results",
             "Result folders in pb7/optimize_results. Folders changed within 24 hours are kept.",
             lambda ctx: _children_items(ctx, _sub(ctx.pb7, "optimize_results"), guard_active=True),
             _pb("pb7", "optimize_results")),
    Category("pb7_backtests", "results", "review", "PB7 backtests",
             "Backtest folders in pb7/backtests/<group>/<config>.",
             lambda ctx: _children_items(ctx, _sub(ctx.pb7, "backtests"), depth=2, guard_active=True),
             _pb("pb7", "backtests")),
    Category("pb8_optimize_results", "results", "review", "PB8 optimize results",
             "Result folders in pb8/optimize_results. Folders changed within 24 hours are kept.",
             lambda ctx: _children_items(ctx, _sub(ctx.pb8, "optimize_results"), guard_active=True),
             _pb("pb8", "optimize_results")),
    Category("pb8_backtests", "results", "review", "PB8 backtests",
             "Backtest folders in pb8/backtests/<group>/<config>.",
             lambda ctx: _children_items(ctx, _sub(ctx.pb8, "backtests"), depth=2, guard_active=True),
             _pb("pb8", "backtests")),
    Category("pb7_hlcvs", "caches", "optional", "PB7 prepared backtest data",
             "pb7/caches/hlcvs_data. PB7 rebuilds a dataset when a backtest needs it again.",
             _list_pb7_hlcvs, _pb("pb7", "caches", "hlcvs_data"), default_days=7, auto_allowed=True,
             runtime_check=_pb7_backtest_running),
    Category("pb7_legacy_history", "caches", "review", "PB7 legacy historical data",
             "pb7/historical_data. PB7 migrated it to caches/ohlcv and leaves the old folder for manual deletion.",
             _list_pb7_legacy, _pb("pb7", "historical_data")),
    Category("pb7_cache_leftovers", "caches", "review", "PB7 cache leftovers",
             "Renamed exchange folders such as *_old or *_org in pb7/caches/ohlcv.",
             _list_pb7_cache_leftovers, _pb("pb7", "caches", "ohlcv")),
    Category("protected_market", "caches", "protected", "Market data",
             "Active market data stores. Shown for information only; manage them in Market Data.",
             _list_protected_market, lambda ctx: []),
    Category("db_backups", "backups", "safe", "Database backups",
             "Automatic pbgui.db backups in data/backup/db.",
             _list_db_backups, _r("backup", "db"), default_days=30, default_keep=5, auto_allowed=True),
    Category("db_tools_backups", "backups", "safe", "DB Tools backups",
             "Restore points created by DB Tools in data/backup/db-tools.",
             _list_db_tools_backups, _r("backup", "db-tools"), default_days=30, default_keep=5, auto_allowed=True),
    Category("db_repair", "backups", "safe", "DB repair leftovers",
             "Working folders of finished database repairs (data/backup/db-repair-*).",
             _list_db_repair, _r("backup"), default_days=14, auto_allowed=True),
    Category("release_worktrees", "backups", "review", "Release worktrees",
             "Git worktrees of older PBGui releases in data/release_worktrees.",
             _list_release_worktrees, _r("release_worktrees"), deleter="worktree"),
    Category("manual_copies", "backups", "review", "Manual copies",
             "Copies such as “instances (Kopie)” in the data folder.",
             _list_manual_copies, lambda ctx: [ctx.data]),
    Category("old_venvs", "backups", "review", "Python environments",
             "Virtual environments next to PBGui. Environments in use are kept.",
             _list_venvs, lambda ctx: [ctx.pbgui.parent], deleter="venv",
             runtime_check=lambda ctx, item: _venv_in_use(ctx, [Path(path) for path in item["paths"]])),
    Category("protected_core", "backups", "protected", "Core data",
             "Database, credentials, API keys and configurations. Never deleted here.",
             _list_protected_core, lambda ctx: []),
)
CATEGORY_BY_ID = {category.id: category for category in CATEGORIES}


def list_category(ctx: StorageContext, category: Category) -> dict[str, Any]:
    """List one category, converting unexpected failures into a blocker."""
    try:
        listing = category.lister(ctx)
    except ScanCancelled:
        raise
    except Exception as exc:
        return {"items": [], "blocked": f"Listing failed: {type(exc).__name__}", "available": True}
    return {"items": listing.items, "blocked": listing.blocked, "available": listing.available}


def select_items(items: list[dict[str, Any]], *, now: float, days: int | None = None,
                 keep: int | None = None, item_ids: list[str] | set[str] | None = None) -> list[dict[str, Any]]:
    """Return the deletable items matching an age, keep-newest and id filter."""
    kept: set[str] = set()
    if keep:
        newest = sorted(items, key=lambda item: float(item.get("sort_mtime") or 0), reverse=True)
        kept = {item["id"] for item in newest[: max(0, int(keep))]}
    wanted = set(item_ids) if item_ids is not None else None
    selected = []
    for item in items:
        if item.get("blocked") or item["id"] in kept:
            continue
        if wanted is not None and item["id"] not in wanted:
            continue
        if days is not None and days > 0 and float(item.get("mtime") or 0) > now - days * DAY:
            continue
        selected.append(item)
    return selected


# ── Scan ──────────────────────────────────────────────────────


def _scan_roots(ctx: StorageContext) -> list[tuple[str, str, Path]]:
    roots = [("pbgui", "PBGui", ctx.pbgui)]
    if ctx.pb7 is not None:
        roots.append(("pb7", "PB7", ctx.pb7))
    if ctx.pb8 is not None:
        roots.append(("pb8", "PB8", ctx.pb8))
    return roots


def _tree(path: str, name: str, ctx: StorageContext, children: dict[str, list[str]], depth: int = 0) -> dict[str, Any]:
    size, files, mtime = ctx.dirstats.get(path, (0, 0, 0.0))
    node: dict[str, Any] = {"name": name, "path": path, "size": size, "files": files, "mtime": mtime}
    subdirs = children.get(path) or []
    if subdirs and depth < TREE_DEPTH:
        nodes = sorted(
            (_tree(sub, os.path.basename(sub), ctx, children, depth + 1) for sub in subdirs),
            key=lambda item: item["size"], reverse=True,
        )
        shown = [item for item in nodes[:TREE_CHILDREN] if item["size"] >= TREE_MIN_BYTES]
        rest = size - sum(item["size"] for item in shown)
        if shown:
            if rest >= TREE_MIN_BYTES:
                shown.append({"name": "Other files", "path": "", "size": rest, "files": 0, "mtime": 0, "other": True})
            node["children"] = shown
    return node


def mount_usage(paths: dict[str, Path]) -> list[dict[str, Any]]:
    """Group root paths by filesystem and return its live usage."""
    import psutil

    mounts: dict[str, dict[str, Any]] = {}
    partitions = sorted(psutil.disk_partitions(all=False), key=lambda part: len(part.mountpoint), reverse=True)
    for root_id, path in paths.items():
        real = _real(path)
        mountpoint = next((part.mountpoint for part in partitions
                           if real == part.mountpoint or real.startswith(part.mountpoint.rstrip(os.sep) + os.sep)), "")
        if not mountpoint:
            mountpoint = os.path.abspath(os.sep)
        entry = mounts.get(mountpoint)
        if entry is None:
            try:
                usage = psutil.disk_usage(real)
            except OSError:
                continue
            entry = {"mount": mountpoint, "total": usage.total, "used": usage.used, "free": usage.free,
                     "percent": usage.percent, "roots": []}
            mounts[mountpoint] = entry
        entry["roots"].append(root_id)
    return list(mounts.values())


def scan(ctx: StorageContext, progress: Callable[[str], None] | None = None) -> dict[str, Any]:
    """Walk all roots and list every category into a snapshot document."""
    started = time.time()
    seen: set[tuple[int, int]] = set()
    children: dict[str, list[str]] = {}
    roots = _scan_roots(ctx)
    venvs = _venv_candidates(ctx)
    root_paths = {str(path) for _, _, path in roots} | {str(path) for path in venvs}
    result_roots = []
    for root_id, label, path in roots:
        if progress:
            progress(f"Scanning {label}")
        skip = root_paths - {str(path)}
        walk_stats(str(path), stop=ctx.stop, seen=seen, dirstats=ctx.dirstats, children=children, skip=skip)
        result_roots.append({"id": root_id, "label": label, "path": str(path),
                             "tree": _tree(str(path), label, ctx, children)})
    if venvs:
        if progress:
            progress("Scanning Python environments")
        venv_nodes = []
        for path in venvs:
            walk_stats(str(path), stop=ctx.stop, seen=seen, dirstats=ctx.dirstats)
            size, files, mtime = ctx.dirstats.get(str(path), (0, 0, 0.0))
            venv_nodes.append({"name": path.name, "path": str(path), "size": size, "files": files, "mtime": mtime})
        venv_nodes.sort(key=lambda node: node["size"], reverse=True)
        result_roots.append({"id": "venvs", "label": "Python environments", "path": str(ctx.pbgui.parent),
                             "tree": {"name": "Python environments", "path": "", "size": sum(n["size"] for n in venv_nodes),
                                      "files": sum(n["files"] for n in venv_nodes), "mtime": 0, "children": venv_nodes}})
    categories = {}
    for category in CATEGORIES:
        if progress:
            progress(f"Listing {category.label}")
        categories[category.id] = list_category(ctx, category)
    mount_paths = {root_id: path for root_id, _, path in roots}
    if venvs:
        mount_paths["venvs"] = venvs[0]
    return {
        "version": SNAPSHOT_VERSION,
        "started_at": started,
        "finished_at": time.time(),
        "roots": result_roots,
        "mounts": mount_usage(mount_paths),
        "categories": categories,
    }


def apply_deletions(snapshot: dict[str, Any], deleted: dict[str, list[dict[str, Any]]]) -> None:
    """Remove deleted items from a snapshot and subtract them from the trees."""
    for category_id, items in deleted.items():
        entry = snapshot.get("categories", {}).get(category_id)
        removed_ids = {item["id"] for item in items}
        if entry:
            entry["items"] = [item for item in entry.get("items", []) if item["id"] not in removed_ids]
        for item in items:
            sizes = item.get("path_sizes") or {path: item.get("size", 0) for path in item.get("paths", [])[:1]}
            for path in item.get("paths", []):
                _subtract_tree(snapshot, path, int(sizes.get(path, 0)))


def _subtract_tree(snapshot: dict[str, Any], path: str, size: int) -> None:
    if size <= 0:
        return
    for root in snapshot.get("roots", []):
        node = root.get("tree") or {}
        base = node.get("path") or root.get("path") or ""
        if root.get("id") == "venvs":
            for child in node.get("children", []):
                if path == child.get("path"):
                    child["size"] = max(0, child["size"] - size)
                    node["size"] = max(0, node["size"] - size)
            continue
        if not base or not (path == base or path.startswith(base.rstrip(os.sep) + os.sep)):
            continue
        current = node
        while current is not None:
            current["size"] = max(0, int(current.get("size", 0)) - size)
            following = None
            for child in current.get("children", []):
                child_path = child.get("path") or ""
                if child_path and (path == child_path or path.startswith(child_path.rstrip(os.sep) + os.sep)):
                    following = child
                    break
            if following is not None and following.get("path") == path:
                current["children"] = [child for child in current["children"] if child is not following]
                break
            current = following


# ── Deletion ──────────────────────────────────────────────────


class DeleteError(Exception):
    """A single item could not be deleted safely."""


class ItemBlocked(DeleteError):
    """The item is in use right now; it is skipped, which is not a failure."""


def _within(path: Path, roots: list[Path]) -> bool:
    """Return whether ``path`` lies strictly below one approved root."""
    try:
        parent = Path(os.path.realpath(path.parent))
    except OSError:
        return False
    for root in roots:
        try:
            real_root = Path(os.path.realpath(root))
        except OSError:
            continue
        if parent == real_root or real_root in parent.parents:
            if Path(os.path.realpath(path)) != real_root or path.is_symlink():
                return True
    return False


class TrashRegistry:
    """Track renamed directories so interrupted deletions finish later."""

    def __init__(self, state_dir: Path) -> None:
        self.path = state_dir / "trash.json"

    def _read(self) -> list[str]:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return []
        return [str(item) for item in value if isinstance(item, str)] if isinstance(value, list) else []

    def _write(self, entries: list[str]) -> None:
        ensure_private_directory(self.path.parent)
        atomic_write_private_text(self.path, json.dumps(sorted(set(entries)), indent=4))

    def add(self, path: Path) -> None:
        with advisory_file_lock(self.path):
            self._write(self._read() + [str(path)])

    def remove(self, path: Path) -> None:
        with advisory_file_lock(self.path):
            self._write([entry for entry in self._read() if entry != str(path)])

    def pending(self) -> list[Path]:
        with advisory_file_lock(self.path):
            return [Path(entry) for entry in self._read() if Path(entry).name.startswith(TRASH_PREFIX)]


def remove_tree(path: Path, stop: Callable[[], bool] | None = None) -> tuple[int, int]:
    """Delete a directory bottom-up without following links; return freed bytes/files."""
    freed = files = 0
    for dirpath, dirnames, filenames in os.walk(path, topdown=False, followlinks=False):
        for name in filenames + [d for d in dirnames if os.path.islink(os.path.join(dirpath, d))]:
            if stop is not None and stop():
                raise ScanCancelled()
            full = os.path.join(dirpath, name)
            try:
                st = os.lstat(full)
                os.unlink(full)
            except FileNotFoundError:
                continue
            freed += allocated_bytes(st)
            files += 1
        for name in dirnames:
            full = os.path.join(dirpath, name)
            if os.path.islink(full):
                continue
            try:
                st = os.lstat(full)
                os.rmdir(full)
                freed += allocated_bytes(st)
            except FileNotFoundError:
                continue
    st = os.lstat(path)
    os.rmdir(path)
    return freed + allocated_bytes(st), files


def delete_path(path: Path, roots: list[Path], trash: TrashRegistry,
                stop: Callable[[], bool] | None = None) -> tuple[int, int]:
    """Delete one file or directory below an approved root."""
    if not _within(path, roots):
        raise DeleteError(f"Refusing path outside its category: {path}")
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return 0, 0
    if not stat.S_ISDIR(st.st_mode):
        os.unlink(path)
        return allocated_bytes(st), 1
    staged = path.with_name(f"{TRASH_PREFIX}{uuid.uuid4().hex[:12]}-{path.name[:40]}")
    trash.add(staged)
    try:
        os.replace(path, staged)
    except OSError:
        trash.remove(staged)
        raise
    freed, files = remove_tree(staged, stop)
    trash.remove(staged)
    return freed, files


def finish_pending_trash(trash: TrashRegistry, stop: Callable[[], bool] | None = None) -> int:
    """Complete deletions interrupted by a stop or crash."""
    freed = 0
    for staged in trash.pending():
        if staged.is_symlink() or not staged.exists():
            trash.remove(staged)
            continue
        if staged.is_dir():
            freed += remove_tree(staged, stop)[0]
        trash.remove(staged)
    return freed


def delete_item(ctx: StorageContext, category: Category, item: dict[str, Any], trash: TrashRegistry) -> tuple[int, int]:
    """Delete one re-listed item with the category's deletion method.

    The category's runtime blocker is checked here, as the last step before
    anything is removed.
    """
    if category.runtime_check is not None:
        reason = category.runtime_check(ctx, item)
        if reason:
            raise ItemBlocked(reason)
    roots = category.roots(ctx)
    paths = [Path(path) for path in item["paths"]]
    if category.deleter == "vast":
        from vast_jobs import JobStore
        from vast_queue import CloudQueue

        ids = [job for job in item.get("meta", {}).get("job_ids", []) if _VAST_ID_RE.match(str(job))]
        if not ids:
            raise DeleteError("Cloud job has no identifiers")
        CloudQueue(JobStore(ctx.data / "vast")).purge_job_histories(ids)
        return int(item.get("size") or 0), int(item.get("files") or 0)
    if category.deleter == "rotated_log":
        from logging_helpers import canonical_log_lock_target

        freed = files = 0
        for path in paths:
            base = item.get("meta", {}).get("base") or str(path)
            with advisory_file_lock(canonical_log_lock_target(base)):
                result = delete_path(path, roots, trash, ctx.stop)
            freed += result[0]
            files += result[1]
        return freed, files
    if category.deleter == "worktree":
        for path in paths:
            if not _within(path, roots):
                raise DeleteError(f"Refusing path outside its category: {path}")
            freed = ctx.stats(path)[0]
            done = subprocess.run(
                ["git", "-C", str(ctx.pbgui), "worktree", "remove", "--force", str(path)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=600, check=False,
            )
            if done.returncode == 0 and not path.exists():
                return freed, int(item.get("files") or 0)
        result = (0, 0)
        for path in paths:
            freed, files = delete_path(path, roots, trash, ctx.stop)
            result = (result[0] + freed, result[1] + files)
        subprocess.run(["git", "-C", str(ctx.pbgui), "worktree", "prune"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120, check=False)
        return result
    if category.deleter == "venv":
        for path in paths:
            if not (path / "pyvenv.cfg").is_file():
                raise DeleteError(f"Not a Python environment: {path.name}")
    freed = files = 0
    for path in paths:
        result = delete_path(path, roots, trash, ctx.stop)
        freed += result[0]
        files += result[1]
    return freed, files
