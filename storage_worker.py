"""Storage scan and cleanup worker.

Spawned by the API storage controller as ``python storage_worker.py <job.json>``.
It runs with low CPU/IO priority so filesystem walks never compete with the
API event loop, holds an exclusive lock so only one worker runs at a time,
and reports progress through ``data/storage/progress.json``. SIGTERM stops
it between files; interrupted directory deletions finish on the next run.
"""

from __future__ import annotations

import dataclasses
import json
import os
import signal
import sys
import time
import traceback
from pathlib import Path
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover - PBGui production targets Linux
    fcntl = None

from logging_helpers import human_log as _log
from secure_files import atomic_write_private_text, ensure_private_directory
import storage_registry as registry

SERVICE = "Storage"
EXIT_BUSY = 75
MAX_ERRORS = 50

_STOP = False


def _handle_stop(signum, frame) -> None:  # noqa: ARG001 - signal handler signature
    global _STOP
    _STOP = True


def _stopped() -> bool:
    return _STOP


def state_dir(pbgdir: Path) -> Path:
    return pbgdir / "data" / "storage"


def write_json(path: Path, value: Any) -> None:
    """Atomically replace a private JSON document."""
    ensure_private_directory(path.parent)
    atomic_write_private_text(path, json.dumps(value, indent=4))


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


class Progress:
    """Throttled progress document shared with the API controller."""

    def __init__(self, path: Path, job: dict[str, Any]) -> None:
        self.path = path
        self.data: dict[str, Any] = {
            "job_id": job["id"], "mode": job["mode"], "status": "running", "phase": "Starting",
            "current": "", "done_items": 0, "total_items": 0, "freed_bytes": 0, "total_bytes": 0,
            "started_at": time.time(), "updated_at": time.time(), "errors": [], "results": {},
        }
        self._written = 0.0
        self.write(force=True)

    def update(self, force: bool = False, **changes: Any) -> None:
        self.data.update(changes)
        self.write(force=force)

    def error(self, text: str) -> None:
        if len(self.data["errors"]) < MAX_ERRORS:
            self.data["errors"].append(text)

    def write(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._written < 0.5:
            return
        self.data["updated_at"] = now
        write_json(self.path, self.data)
        self._written = now


def _lower_priority() -> None:
    try:
        os.nice(10)
    except (AttributeError, OSError):
        pass
    try:
        import psutil

        process = psutil.Process()
        if hasattr(psutil, "IOPRIO_CLASS_IDLE"):
            process.ionice(psutil.IOPRIO_CLASS_IDLE)
    except Exception:
        pass


def _run_scan(ctx: registry.StorageContext, progress: Progress, sdir: Path, job: dict[str, Any]) -> dict[str, Any]:
    progress.update(phase="Scanning", force=True)
    snapshot = registry.scan(ctx, progress=lambda text: progress.update(current=text))
    snapshot["job_id"] = job["id"]
    write_json(sdir / "snapshot.json", snapshot)
    return snapshot


class _Recheck:
    """Validate each item again right before it is deleted.

    Folders deleted earlier in the same run can take long, so the plan's
    statistics and time are not enough. Every item is measured again; when it
    changed since the plan, the category is listed again for this item with
    fresh statistics and the current time, so activity guards and age filters
    apply exactly like in the preview. An unchanged item only grows older, so
    its plan result still holds. Blockers that do not touch the item's files
    (queued jobs, running PB7, used environments) are the category's
    ``runtime_check``, which ``delete_item`` runs before every deletion.
    """

    def __init__(self, ctx: registry.StorageContext, category: registry.Category,
                 days: int | None, keep: int | None) -> None:
        self.ctx = ctx
        self.category = category
        self.days = days
        self.keep = keep

    def _changed(self, item: dict) -> bool:
        fresh = dataclasses.replace(self.ctx, dirstats={}, fresh_stats=True, only_ids=None)
        newest = 0.0
        for path in item.get("paths") or []:
            try:
                os.lstat(path)
            except OSError:
                return True
            newest = max(newest, fresh.stats(Path(path))[2])
        return newest > float(item.get("mtime") or 0)

    def check(self, item: dict) -> dict | None:
        """Return the current item when it may still be deleted, else None."""
        if not self._changed(item):
            return item
        now = time.time()
        fresh = dataclasses.replace(self.ctx, now=now, dirstats={}, fresh_stats=True,
                                    only_ids=None if self.keep else {str(item["id"])})
        listing = registry.list_category(fresh, self.category)
        if listing["blocked"]:
            return None
        items = registry.select_items(listing["items"], now=now, days=self.days, keep=self.keep,
                                      item_ids=[item["id"]])
        return items[0] if items else None


def _skip(progress: Progress, result: dict[str, Any], item: dict) -> None:
    result["skipped"] = result.get("skipped", 0) + 1
    progress.data["skipped_items"] = progress.data.get("skipped_items", 0) + 1
    progress.update(total_items=progress.data["total_items"] - 1,
                    total_bytes=progress.data["total_bytes"] - int(item.get("size") or 0))


def _delete_selected(ctx: registry.StorageContext, progress: Progress, plan: list[tuple[registry.Category, list[dict], dict]],
                     trash: registry.TrashRegistry) -> dict[str, list[dict]]:
    """Delete the selected items in order and record per-category results."""
    total_items = sum(len(items) for _, items, _ in plan)
    total_bytes = sum(int(item.get("size") or 0) for _, items, _ in plan for item in items)
    progress.update(phase="Deleting", total_items=total_items, total_bytes=total_bytes, force=True)
    deleted: dict[str, list[dict]] = {}
    for category, items, selection in plan:
        result = progress.data["results"].setdefault(category.id, {"deleted": 0, "freed": 0, "failed": 0})
        recheck = _Recheck(ctx, category, selection.get("days"), selection.get("keep"))
        for item in items:
            if _STOP:
                return deleted
            progress.update(current=f"{category.label}: {item['label']}")
            try:
                current = recheck.check(item)
                if current is None:
                    _skip(progress, result, item)
                    continue
                freed, _files = registry.delete_item(ctx, category, current, trash)
            except registry.ScanCancelled:
                return deleted
            except registry.ItemBlocked as exc:
                _skip(progress, result, item)
                _log(SERVICE, f"cleanup skipped {category.id}/{item['id']}: {exc}")
                continue
            except Exception as exc:
                result["failed"] += 1
                progress.error(f"{category.label}: {item['label']}: {type(exc).__name__}: {exc}")
                _log(SERVICE, f"cleanup failed for {category.id}/{item['id']}: {type(exc).__name__}: {exc}", level="WARNING")
                continue
            result["deleted"] += 1
            result["freed"] += int(freed)
            deleted.setdefault(category.id, []).append(current)
            progress.update(done_items=progress.data["done_items"] + 1,
                            freed_bytes=progress.data["freed_bytes"] + int(freed))
    return deleted


def _cleanup_plan(ctx: registry.StorageContext, selections: list[dict[str, Any]],
                  progress: Progress) -> list[tuple[registry.Category, list[dict], dict]]:
    """Re-list each selected category from disk and apply the saved filters."""
    plan = []
    for selection in selections:
        category = registry.CATEGORY_BY_ID.get(str(selection.get("category")))
        if category is None or category.tier == "protected":
            continue
        progress.update(phase="Checking", current=category.label)
        item_ids = selection.get("item_ids")
        keep = selection.get("keep")
        ctx.only_ids = None if keep or item_ids is None else {str(item) for item in item_ids}
        listing = registry.list_category(ctx, category)
        if listing["blocked"]:
            progress.error(f"{category.label}: {listing['blocked']}")
            continue
        items = registry.select_items(listing["items"], now=ctx.now, days=selection.get("days"),
                                      keep=keep, item_ids=item_ids)
        if item_ids is not None and len(items) < len(item_ids):
            skipped = len(item_ids) - len(items)
            result = progress.data["results"].setdefault(category.id, {"deleted": 0, "freed": 0, "failed": 0})
            result["skipped"] = skipped
            progress.data["skipped_items"] = progress.data.get("skipped_items", 0) + skipped
        plan.append((category, items, selection))
    ctx.only_ids = None
    return plan


def _rule_selections(rules: dict[str, Any]) -> list[dict[str, Any]]:
    selections = []
    for category_id, rule in (rules or {}).items():
        category = registry.CATEGORY_BY_ID.get(category_id)
        if category is None or not category.auto_allowed or not isinstance(rule, dict) or not rule.get("enabled"):
            continue
        selections.append({"category": category_id, "days": rule.get("days"), "keep": rule.get("keep"), "item_ids": None})
    return selections


def _finish_snapshot(sdir: Path, snapshot: dict[str, Any] | None, deleted: dict[str, list[dict]]) -> None:
    """Remove deleted items from the stored snapshot without a full rescan."""
    snapshot = snapshot if snapshot is not None else read_json(sdir / "snapshot.json")
    if not isinstance(snapshot, dict) or not deleted:
        return
    registry.apply_deletions(snapshot, deleted)
    try:
        paths = {root["id"]: Path(root["path"]) for root in snapshot.get("roots", []) if root.get("id") != "venvs"}
        snapshot["mounts"] = registry.mount_usage(paths) or snapshot.get("mounts", [])
    except Exception:
        pass
    snapshot["updated_at"] = time.time()
    write_json(sdir / "snapshot.json", snapshot)


def run(job_path: Path, pbgdir: Path, context_factory=None) -> int:
    """Execute one storage job and return the process exit code."""
    job = read_json(job_path)
    if not isinstance(job, dict) or job.get("mode") not in {"scan", "cleanup", "auto"}:
        return 2
    sdir = state_dir(pbgdir)
    ensure_private_directory(sdir)
    lock_fd = os.open(sdir / "worker.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        if fcntl is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return EXIT_BUSY
        progress = Progress(sdir / "progress.json", job)
        trash = registry.TrashRegistry(sdir)
        status = "done"
        try:
            progress.update(phase="Finishing earlier deletions", force=True)
            registry.finish_pending_trash(trash, _stopped)
            factory = context_factory or registry.StorageContext.from_settings
            ctx = factory(pbgdir, stop=_stopped)
            snapshot = None
            if job["mode"] in {"scan", "auto"}:
                snapshot = _run_scan(ctx, progress, sdir, job)
            if job["mode"] in {"cleanup", "auto"}:
                if job["mode"] == "auto":
                    selections = _rule_selections(job.get("rules") or {})
                    plan = []
                    for selection in selections:
                        category = registry.CATEGORY_BY_ID[selection["category"]]
                        listing = snapshot["categories"].get(category.id, {})
                        if listing.get("blocked"):
                            continue
                        plan.append((category, registry.select_items(
                            listing.get("items", []), now=ctx.now, days=selection["days"], keep=selection["keep"]),
                            selection))
                else:
                    ctx.fresh_stats = True
                    plan = _cleanup_plan(ctx, job.get("selections") or [], progress)
                deleted = _delete_selected(ctx, progress, plan, trash)
                _finish_snapshot(sdir, snapshot, deleted)
            if _STOP:
                status = "cancelled"
        except registry.ScanCancelled:
            status = "cancelled"
        except Exception as exc:
            status = "failed"
            progress.error(f"{type(exc).__name__}: {exc}")
            _log(SERVICE, f"storage {job['mode']} failed: {type(exc).__name__}: {exc}", level="ERROR",
                 meta={"traceback": traceback.format_exc()})
        progress.update(status=status, phase=status.capitalize(), current="", finished_at=time.time(), force=True)
        if job["mode"] != "scan" or status != "done":
            data = progress.data
            _log(SERVICE, f"storage {job['mode']} {status}: deleted {data['done_items']} item(s), "
                          f"freed {data['freed_bytes']} bytes, {len(data['errors'])} error(s)",
                 level="INFO" if status == "done" else "WARNING")
        return 0 if status in {"done", "cancelled"} else 1
    finally:
        os.close(lock_fd)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        return 2
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    _lower_priority()
    from pbgui_purefunc import PBGDIR

    return run(Path(argv[1]), Path(PBGDIR))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
