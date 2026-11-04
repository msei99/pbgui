"""API-owned controller for the Storage page.

It owns at most one storage worker subprocess, schedules automatic scans and
retention cleanups, keeps the latest snapshot in memory and turns browser
selections into short-lived cleanup plans. Only the worker writes the
snapshot; only the controller writes settings, history and the last result.
"""

from __future__ import annotations

import asyncio
import json
import signal
import sys
import time
import uuid
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from secure_files import atomic_write_private_text, ensure_private_directory
import storage_registry as registry

SERVICE = "Storage"

DEFAULT_SCAN_INTERVAL_H = 6
VIEW_MAX_AGE_S = 3600
VIEW_SCAN_COOLDOWN_S = 600
SCHEDULER_INTERVAL_S = 60
FIRST_TICK_DELAY_S = 45
PLAN_TTL_S = 1800
MAX_PLANS = 20
PLAN_DISPLAY_ITEMS = 20_000
HISTORY_LIMIT = 800
STOP_GRACE_S = 10
MAX_DAYS = 3650
MAX_KEEP = 1000
MAX_ITEM_IDS = 100_000
DISK_WARN_PERCENT = 85
DISK_CRITICAL_PERCENT = 95


class StorageBusy(Exception):
    """A storage worker is already running."""


class StorageError(Exception):
    """A request cannot be served; ``status`` maps to an HTTP status."""

    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


def default_settings() -> dict[str, Any]:
    """Return settings with automatic cleanup disabled."""
    rules = {}
    for category in registry.CATEGORIES:
        if category.auto_allowed:
            rules[category.id] = {"enabled": category.tier == "safe", "days": category.default_days or 0,
                                  "keep": category.default_keep or 0}
    return {"scan_interval_hours": DEFAULT_SCAN_INTERVAL_H, "auto": {"enabled": False, "hour": 4, "rules": rules}}


def validate_settings(value: Any) -> dict[str, Any]:
    """Validate a complete settings document from the browser."""
    if not isinstance(value, dict):
        raise StorageError("Settings must be an object")
    result = default_settings()
    interval = value.get("scan_interval_hours", DEFAULT_SCAN_INTERVAL_H)
    if type(interval) is not int or not 1 <= interval <= 48:
        raise StorageError("Scan interval must be between 1 and 48 hours")
    result["scan_interval_hours"] = interval
    auto = value.get("auto") or {}
    if not isinstance(auto, dict):
        raise StorageError("Automatic cleanup settings must be an object")
    hour = auto.get("hour", 4)
    if type(hour) is not int or not 0 <= hour <= 23:
        raise StorageError("Cleanup hour must be between 0 and 23")
    result["auto"]["enabled"] = bool(auto.get("enabled"))
    result["auto"]["hour"] = hour
    rules = auto.get("rules") or {}
    if not isinstance(rules, dict):
        raise StorageError("Cleanup rules must be an object")
    for category_id, rule in rules.items():
        category = registry.CATEGORY_BY_ID.get(category_id)
        if category is None or not category.auto_allowed:
            raise StorageError(f"Automatic cleanup is not available for {category_id}")
        if not isinstance(rule, dict):
            raise StorageError("Each cleanup rule must be an object")
        days = _bounded_int(rule.get("days", 0), "days", 0, MAX_DAYS)
        keep = _bounded_int(rule.get("keep", 0), "keep", 0, MAX_KEEP)
        if days == 0 and keep == 0 and rule.get("enabled"):
            raise StorageError(f"{category.label}: set an age or a number of entries to keep")
        result["auto"]["rules"][category_id] = {"enabled": bool(rule.get("enabled")), "days": days, "keep": keep}
    return result


def _bounded_int(value: Any, name: str, low: int, high: int) -> int:
    if value is None:
        return 0
    if type(value) is not int or not low <= value <= high:
        raise StorageError(f"{name} must be a whole number between {low} and {high}")
    return value


def _validate_exclude(value: Any) -> dict[str, set[str]]:
    """Validate ``{category: [item ids]}`` sent to deselect preview entries."""
    if value is None:
        return {}
    if not isinstance(value, dict) or len(value) > len(registry.CATEGORIES):
        raise StorageError("Invalid deselection")
    result = {}
    for category_id, items in value.items():
        if category_id not in registry.CATEGORY_BY_ID or not isinstance(items, list) or len(items) > MAX_ITEM_IDS \
                or not all(isinstance(item, str) for item in items):
            raise StorageError("Invalid deselection")
        result[category_id] = set(items)
    return result


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def _write_json(path: Path, value: Any) -> None:
    ensure_private_directory(path.parent)
    atomic_write_private_text(path, json.dumps(value, indent=4))


def _public_item(item: dict[str, Any]) -> dict[str, Any]:
    return {key: item.get(key) for key in ("id", "label", "paths", "size", "files", "mtime", "blocked")}


class StorageController:
    """Owns the storage worker lifecycle and the in-memory snapshot."""

    def __init__(self, pbgdir: Path, *, python: str = sys.executable, worker_script: Path | None = None) -> None:
        self.pbgdir = Path(pbgdir)
        self.state_dir = self.pbgdir / "data" / "storage"
        self.python = python
        self.worker_script = worker_script or self.pbgdir / "storage_worker.py"
        self._snapshot: dict[str, Any] | None = None
        self._snapshot_mtime = 0.0
        self._generation = 0
        self._loaded = False
        self._proc: asyncio.subprocess.Process | None = None
        self._job: dict[str, Any] | None = None
        self._progress: dict[str, Any] | None = None
        self._last_result: dict[str, Any] | None = None
        self._monitor_task: asyncio.Task | None = None
        self._scheduler_task: asyncio.Task | None = None
        self._plans: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._lock = asyncio.Lock()
        self._closed = False
        self._last_scan_request = 0.0
        self._busy_elsewhere_until = 0.0

    # ── lifecycle ──

    def start(self) -> None:
        """Start the scheduler on the running event loop."""
        if self._scheduler_task is None:
            self._closed = False
            self._scheduler_task = asyncio.get_running_loop().create_task(self._scheduler_loop(), name="storage-scheduler")

    async def shutdown(self) -> None:
        """Stop the scheduler and the worker; idempotent."""
        self._closed = True
        scheduler, self._scheduler_task = self._scheduler_task, None
        if scheduler is not None:
            scheduler.cancel()
            await asyncio.gather(scheduler, return_exceptions=True)
        await self._stop_worker()
        monitor, self._monitor_task = self._monitor_task, None
        if monitor is not None:
            # Let the monitor record the final result of the stopped worker first.
            await asyncio.wait({monitor}, timeout=STOP_GRACE_S)
            if not monitor.done():
                monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)

    async def _stop_worker(self) -> None:
        proc = self._proc
        if proc is None or proc.returncode is not None:
            return
        try:
            proc.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(proc.wait(), STOP_GRACE_S)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()

    async def _scheduler_loop(self) -> None:
        await self.ensure_loaded()
        await asyncio.sleep(FIRST_TICK_DELAY_S)
        while not self._closed:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log(SERVICE, f"scheduler tick failed: {type(exc).__name__}: {exc}", level="ERROR")
            await asyncio.sleep(SCHEDULER_INTERVAL_S)

    async def _tick(self) -> None:
        if self.busy or time.time() < self._busy_elsewhere_until:
            return
        settings = await asyncio.to_thread(self.load_settings)
        now = datetime.now()
        auto = settings["auto"]
        if auto["enabled"] and now.hour >= auto["hour"]:
            last = float(_read_json(self.state_dir / "auto_state.json", {}).get("last_run", 0) or 0)
            if datetime.fromtimestamp(last).date() != now.date():
                try:
                    await self._start_job("auto", {"rules": auto["rules"]})
                except StorageBusy:
                    return
                await asyncio.to_thread(_write_json, self.state_dir / "auto_state.json", {"last_run": time.time()})
                return
        finished = float((self._snapshot or {}).get("finished_at") or 0)
        if time.time() - finished >= settings["scan_interval_hours"] * 3600:
            await self.request_scan()

    async def ensure_loaded(self) -> None:
        """Load the stored snapshot and last result once."""
        if self._loaded:
            return
        self._loaded = True
        await asyncio.to_thread(self._reload_snapshot)
        self._last_result = await asyncio.to_thread(_read_json, self.state_dir / "last_result.json", None)

    def _reload_snapshot(self) -> bool:
        """Load snapshot.json if it changed; return whether it did."""
        path = self.state_dir / "snapshot.json"
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return False
        if mtime == self._snapshot_mtime and self._snapshot is not None:
            return False
        value = _read_json(path, None)
        if not isinstance(value, dict) or value.get("version") != registry.SNAPSHOT_VERSION:
            return False
        self._snapshot = value
        self._snapshot_mtime = mtime
        self._generation += 1
        return True

    # ── jobs ──

    @property
    def busy(self) -> bool:
        return self._proc is not None and self._proc.returncode is None

    async def request_scan(self, *, max_age: float | None = None) -> bool:
        """Start a scan when idle and the snapshot is older than ``max_age``."""
        if self.busy or self._closed:
            return False
        if max_age is not None:
            finished = float((self._snapshot or {}).get("finished_at") or 0)
            if time.time() - finished < max_age or time.time() - self._last_scan_request < VIEW_SCAN_COOLDOWN_S:
                return False
        self._last_scan_request = time.time()
        try:
            await self._start_job("scan", {})
        except StorageBusy:
            return False
        return True

    async def _start_job(self, mode: str, payload: dict[str, Any]) -> dict[str, Any]:
        async with self._lock:
            if self.busy:
                raise StorageBusy()
            if self._closed:
                raise StorageError("PBGui is shutting down", 409)
            job = {"id": uuid.uuid4().hex, "mode": mode, "created_at": time.time(), **payload}
            job_path = self.state_dir / "jobs" / f"{job['id']}.json"
            await asyncio.to_thread(self._prepare_job_file, job_path, job)
            proc = await asyncio.create_subprocess_exec(
                self.python, str(self.worker_script), str(job_path),
                cwd=str(self.pbgdir), stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            self._proc = proc
            self._job = {"id": job["id"], "mode": mode, "started_at": job["created_at"]}
            self._progress = None
            self._monitor_task = asyncio.get_running_loop().create_task(
                self._monitor(proc, job, job_path), name=f"storage-{mode}")
            return dict(self._job)

    def _prepare_job_file(self, job_path: Path, job: dict[str, Any]) -> None:
        jobs = job_path.parent
        ensure_private_directory(self.state_dir)
        ensure_private_directory(jobs)
        for old in jobs.glob("*.json"):
            try:
                old.unlink()
            except OSError:
                pass
        _write_json(job_path, job)

    async def _monitor(self, proc: asyncio.subprocess.Process, job: dict[str, Any], job_path: Path) -> None:
        progress_path = self.state_dir / "progress.json"
        try:
            while proc.returncode is None:
                try:
                    await asyncio.wait_for(proc.wait(), 0.5)
                except asyncio.TimeoutError:
                    pass
                progress = await asyncio.to_thread(_read_json, progress_path, None)
                if isinstance(progress, dict) and progress.get("job_id") == job["id"]:
                    self._progress = progress
            await self._finish(proc.returncode, job, progress_path)
        finally:
            try:
                await asyncio.to_thread(job_path.unlink, True)
            except OSError:
                pass

    async def _finish(self, returncode: int | None, job: dict[str, Any], progress_path: Path) -> None:
        progress = await asyncio.to_thread(_read_json, progress_path, None)
        if not isinstance(progress, dict) or progress.get("job_id") != job["id"]:
            progress = {"job_id": job["id"], "mode": job["mode"], "status": "failed", "errors": [], "results": {}}
            if returncode == 75:
                progress["errors"].append("Another storage worker is running")
                self._busy_elsewhere_until = time.time() + 300
                if job["mode"] == "auto":
                    await asyncio.to_thread(_write_json, self.state_dir / "auto_state.json", {"last_run": 0})
            else:
                progress["errors"].append(f"Storage worker exited with code {returncode}")
        progress.setdefault("finished_at", time.time())
        self._progress = progress
        changed = await asyncio.to_thread(self._reload_snapshot)
        if changed and self._snapshot is not None:
            await asyncio.to_thread(self._record_history, self._snapshot)
        if job["mode"] != "scan" or progress.get("status") != "done":
            self._last_result = progress
            await asyncio.to_thread(_write_json, self.state_dir / "last_result.json", progress)
        if job["mode"] == "scan" and progress.get("status") == "done":
            self._last_scan_request = time.time()

    async def cancel(self) -> bool:
        """Ask the running worker to stop."""
        proc = self._proc
        if proc is None or proc.returncode is not None:
            return False
        try:
            proc.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            return False
        return True

    # ── views ──

    def status(self) -> dict[str, Any]:
        """Return the lightweight state polled by the browser."""
        snapshot = self._snapshot or {}
        progress = self._progress if self.busy else None
        return {
            "busy": self.busy,
            "job": dict(self._job) if self.busy and self._job else None,
            "progress": progress,
            "generation": self._generation,
            "scanned_at": snapshot.get("finished_at"),
            "updated_at": snapshot.get("updated_at") or snapshot.get("finished_at"),
            "scan_duration_s": (snapshot.get("finished_at") or 0) - (snapshot.get("started_at") or 0) if snapshot else None,
            "last_result": self._last_result,
        }

    def _items(self, category_id: str) -> tuple[list[dict[str, Any]], str | None, bool]:
        entry = ((self._snapshot or {}).get("categories") or {}).get(category_id) or {}
        return list(entry.get("items") or []), entry.get("blocked"), bool(entry.get("available", True))

    def snapshot_view(self) -> dict[str, Any]:
        """Return trees and per-category summaries of the current snapshot."""
        if self._snapshot is None:
            return {"generation": self._generation, "ready": False}
        now = time.time()
        categories = []
        tiers = {tier: {"bytes": 0, "count": 0} for tier in registry.TIERS}
        for category in registry.CATEGORIES:
            items, blocked, available = self._items(category.id)
            deletable = [item for item in items if not item.get("blocked")]
            mtimes = [float(item.get("mtime") or 0) for item in items if item.get("mtime")]
            if category.tier in {"safe", "optional"} and not blocked:
                match = registry.select_items(items, now=now, days=category.default_days, keep=category.default_keep)
            elif category.tier == "review" and not blocked:
                match = deletable
            else:
                match = []
            match_bytes = sum(int(item.get("size") or 0) for item in match)
            tiers[category.tier]["bytes"] += match_bytes if category.tier != "protected" else sum(int(i.get("size") or 0) for i in items)
            tiers[category.tier]["count"] += len(match)
            categories.append({
                **category.public(),
                "available": available,
                "blocked": blocked,
                "count": len(items),
                "bytes": sum(int(item.get("size") or 0) for item in items),
                "deletable_count": len(deletable),
                "oldest": min(mtimes) if mtimes else None,
                "newest": max(mtimes) if mtimes else None,
                "default_count": len(match),
                "default_bytes": match_bytes,
            })
        snapshot = self._snapshot
        return {
            "ready": True,
            "generation": self._generation,
            "scanned_at": snapshot.get("finished_at"),
            "updated_at": snapshot.get("updated_at") or snapshot.get("finished_at"),
            "roots": snapshot.get("roots") or [],
            "mounts": snapshot.get("mounts") or [],
            "groups": registry.GROUPS,
            "categories": categories,
            "tiers": tiers,
        }

    def category_items(self, category_id: str, *, days: int | None = None, keep: int | None = None,
                       limit: int | None = None) -> dict[str, Any]:
        """Return a category's items with the given filter applied as a flag."""
        category = registry.CATEGORY_BY_ID.get(category_id)
        if category is None:
            raise StorageError("Unknown storage category", 404)
        if self._snapshot is None:
            raise StorageError("The first storage scan has not finished yet", 409)
        days = _bounded_int(days, "days", 0, MAX_DAYS)
        keep = _bounded_int(keep, "keep", 0, MAX_KEEP)
        items, blocked, available = self._items(category_id)
        matched_items = [] if blocked else registry.select_items(items, now=time.time(), days=days, keep=keep)
        matched = {item["id"] for item in matched_items}
        rows = []
        ordered = sorted(items, key=lambda value: int(value.get("size") or 0), reverse=True)
        for item in ordered if limit is None else ordered[: max(0, limit)]:
            row = _public_item(item)
            row["match"] = item["id"] in matched
            rows.append(row)
        return {"category": category.public(), "blocked": blocked, "available": available, "items": rows,
                "total": len(items), "match_count": len(matched_items),
                "match_bytes": sum(int(item.get("size") or 0) for item in matched_items),
                "generation": self._generation}

    # ── plans ──

    def preview(self, selections: Any) -> dict[str, Any]:
        """Build a cleanup plan from validated selections of the snapshot."""
        if self._snapshot is None:
            raise StorageError("The first storage scan has not finished yet", 409)
        if not isinstance(selections, list) or not selections:
            raise StorageError("Choose at least one category")
        now = time.time()
        plan_selections = []
        plan_categories = []
        seen: set[str] = set()
        for raw in selections:
            if not isinstance(raw, dict):
                raise StorageError("Invalid selection")
            category = registry.CATEGORY_BY_ID.get(str(raw.get("category") or ""))
            if category is None:
                raise StorageError("Unknown storage category", 404)
            if category.tier == "protected":
                raise StorageError(f"{category.label} is protected", 403)
            if category.id in seen:
                raise StorageError(f"{category.label} was selected twice")
            seen.add(category.id)
            days = _bounded_int(raw.get("days"), "days", 0, MAX_DAYS)
            keep = _bounded_int(raw.get("keep"), "keep", 0, MAX_KEEP)
            item_ids = raw.get("item_ids")
            if item_ids is not None:
                if not isinstance(item_ids, list) or len(item_ids) > MAX_ITEM_IDS or not all(isinstance(x, str) for x in item_ids):
                    raise StorageError("Invalid item selection")
            elif category.tier == "review":
                raise StorageError(f"{category.label}: select the entries to delete")
            items, blocked, _available = self._items(category.id)
            selected = [] if blocked else registry.select_items(items, now=now, days=days, keep=keep, item_ids=item_ids)
            plan_selections.append({"category": category.id, "days": days, "keep": keep,
                                    "item_ids": [item["id"] for item in selected]})
            plan_categories.append({
                "category": category.id, "label": category.label, "tier": category.tier, "blocked": blocked,
                "count": len(selected), "bytes": sum(int(item.get("size") or 0) for item in selected),
                "items": [_public_item(item) for item in
                          sorted(selected, key=lambda value: int(value.get("size") or 0), reverse=True)[:PLAN_DISPLAY_ITEMS]],
            })
        plan = {
            "id": uuid.uuid4().hex,
            "created_at": now,
            "generation": self._generation,
            "selections": plan_selections,
            "categories": plan_categories,
            "count": sum(entry["count"] for entry in plan_categories),
            "bytes": sum(entry["bytes"] for entry in plan_categories),
        }
        self._prune_plans(now)
        self._plans[plan["id"]] = plan
        while len(self._plans) > MAX_PLANS:
            self._plans.popitem(last=False)
        return {key: value for key, value in plan.items() if key != "selections"}

    def _prune_plans(self, now: float) -> None:
        for plan_id in [key for key, plan in self._plans.items() if now - plan["created_at"] > PLAN_TTL_S]:
            self._plans.pop(plan_id, None)

    async def start_cleanup(self, plan_id: Any, exclude: Any = None) -> dict[str, Any]:
        """Run a previewed plan, optionally without entries deselected in the preview.

        ``exclude`` maps category ids to item ids. It can only remove entries from
        the plan; the worker re-checks every remaining entry on disk.
        """
        if not isinstance(plan_id, str):
            raise StorageError("Invalid cleanup plan")
        excluded = _validate_exclude(exclude)
        self._prune_plans(time.time())
        plan = self._plans.get(plan_id)
        if plan is None:
            raise StorageError("The preview expired. Create a new preview.", 410)
        selections = []
        for selection in plan["selections"]:
            skip = excluded.get(selection["category"], set())
            item_ids = [item for item in selection["item_ids"] if item not in skip]
            if item_ids:
                selections.append({**selection, "item_ids": item_ids})
        count = sum(len(selection["item_ids"]) for selection in selections)
        if not count:
            raise StorageError("Nothing is selected for deletion")
        try:
            job = await self._start_job("cleanup", {"selections": selections})
        except StorageBusy:
            raise StorageError("A storage scan or cleanup is already running", 409) from None
        self._plans.pop(plan_id, None)
        _log(SERVICE, f"cleanup started: {count} item(s) in "
                      f"{', '.join(selection['category'] for selection in selections)}", level="INFO")
        return job

    # ── settings and history ──

    def load_settings(self) -> dict[str, Any]:
        value = _read_json(self.state_dir / "settings.json", None)
        if value is None:
            return default_settings()
        try:
            return validate_settings(value)
        except StorageError:
            return default_settings()

    def save_settings(self, value: Any) -> dict[str, Any]:
        settings = validate_settings(value)
        path = self.state_dir / "settings.json"
        with advisory_file_lock(path):
            _write_json(path, settings)
        return settings

    def auto_state(self) -> dict[str, Any]:
        return _read_json(self.state_dir / "auto_state.json", {}) or {}

    def _record_history(self, snapshot: dict[str, Any]) -> None:
        path = self.state_dir / "history.json"
        stamp = float(snapshot.get("updated_at") or snapshot.get("finished_at") or time.time())
        point = {
            "date": datetime.fromtimestamp(stamp).strftime("%Y-%m-%d"),
            "ts": stamp,
            "mounts": [{"mount": m.get("mount"), "used": m.get("used"), "total": m.get("total")}
                       for m in snapshot.get("mounts") or []],
            "roots": {root["id"]: int((root.get("tree") or {}).get("size") or 0) for root in snapshot.get("roots") or []},
            "areas": {},
        }
        for root in snapshot.get("roots") or []:
            for child in ((root.get("tree") or {}).get("children") or [])[:12]:
                if not child.get("other"):
                    point["areas"][f"{root['label']}/{child['name']}"] = int(child.get("size") or 0)
        with advisory_file_lock(path):
            history = _read_json(path, [])
            history = [entry for entry in history if isinstance(entry, dict) and entry.get("date") != point["date"]]
            history.append(point)
            history.sort(key=lambda entry: float(entry.get("ts") or 0))
            _write_json(path, history[-HISTORY_LIMIT:])

    def history(self) -> list[dict[str, Any]]:
        value = _read_json(self.state_dir / "history.json", [])
        return value if isinstance(value, list) else []

    # ── summary ──

    def reclaimable(self) -> dict[str, int]:
        view = self.snapshot_view()
        if not view.get("ready"):
            return {}
        return {tier: int(view["tiers"][tier]["bytes"]) for tier in ("safe", "optional", "review")}

    def warnings(self, mounts: list[dict[str, Any]], system: dict[str, Any] | None) -> list[dict[str, str]]:
        """Return disk/memory warnings for the Welcome page."""
        warnings = []
        for mount in mounts:
            percent = float(mount.get("percent") or 0)
            if percent >= DISK_WARN_PERCENT:
                warnings.append({
                    "level": "critical" if percent >= DISK_CRITICAL_PERCENT else "warning",
                    "text": f"Disk {mount.get('mount')} is {percent:.0f}% full ({_gb(mount.get('free'))} free)",
                })
        if system:
            memory = system.get("memory") or {}
            if memory.get("total") and memory.get("available", 0) / memory["total"] < 0.1:
                warnings.append({"level": "warning", "text": f"Only {_gb(memory.get('available'))} memory available"})
            swap = system.get("swap") or {}
            if swap.get("total") and float(swap.get("percent") or 0) >= 50:
                warnings.append({"level": "warning", "text": f"Swap is {float(swap['percent']):.0f}% used"})
        return warnings


def _gb(value: Any) -> str:
    try:
        return f"{float(value) / 1024 ** 3:.1f} GB"
    except (TypeError, ValueError):
        return "?"


def live_mounts(controller: StorageController) -> list[dict[str, Any]]:
    """Return live disk usage for the roots of the current snapshot."""
    snapshot = controller._snapshot or {}
    paths = {root["id"]: Path(root["path"]) for root in snapshot.get("roots") or [] if root.get("id") != "venvs"}
    if not paths:
        paths = {"pbgui": controller.pbgdir}
    return registry.mount_usage(paths)
