"""Persistent background collection of PBGui, PB7, and PB8 upstream heads."""

from __future__ import annotations

import asyncio
import copy
import json
import subprocess
import time
from pathlib import Path
from typing import Any, Callable

from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR, pb7dir, pb8dir
from secure_files import atomic_write_private_text, ensure_private_directory


SERVICE = "VPSMonitor"
SCHEMA_VERSION = 1
DEFAULT_INTERVAL_SECONDS = 60.0
PASSIVBOT_UPSTREAM_URL = "https://github.com/enarjord/passivbot.git"
PBGUI_UPSTREAM_URL = "https://github.com/msei99/pbgui.git"


class UpstreamReleaseCollector:
    """Collect remote branch heads without blocking API or browser request paths."""

    def __init__(
        self,
        *,
        pbgui_dir: Path | None = None,
        state_path: Path | None = None,
        interval: float = DEFAULT_INTERVAL_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.pbgui_dir = Path(pbgui_dir or PBGDIR)
        self.state_path = Path(
            state_path
            or self.pbgui_dir / "data" / "state" / "vps_monitor" / "upstream_releases.json"
        )
        self.interval = max(float(interval), 5.0)
        self.clock = clock
        self._snapshot = self._load_snapshot()
        self._task: asyncio.Task | None = None
        self._loop_ref: asyncio.AbstractEventLoop | None = None
        self._wake = asyncio.Event()

    def snapshot(self) -> dict[str, Any]:
        """Return an isolated last-known snapshot for RPC consumers."""
        return copy.deepcopy(self._snapshot)

    async def start(self) -> None:
        """Start one immediate collection followed by periodic refreshes."""
        if self._task is not None and not self._task.done():
            return
        self._loop_ref = asyncio.get_running_loop()
        self._task = asyncio.create_task(self._loop(), name="vps-upstream-release-collector")

    async def stop(self) -> None:
        """Cancel and await the owned collector task."""
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._loop_ref = None

    def request_refresh(self) -> None:
        """Wake the collector after an update without running Git in the caller."""
        loop = self._loop_ref
        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(self._wake.set)
        else:
            self._wake.set()

    async def _loop(self) -> None:
        try:
            while True:
                self._wake.clear()
                worker = asyncio.create_task(
                    asyncio.to_thread(self._collect_and_persist_sync),
                    name="vps-upstream-release-probe",
                )
                try:
                    updated = await asyncio.shield(worker)
                    self._snapshot = updated
                except asyncio.CancelledError:
                    await asyncio.gather(worker, return_exceptions=True)
                    raise
                except Exception as exc:
                    _log(SERVICE, f"Upstream release collection failed: {type(exc).__name__}", level="WARNING")
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.interval)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            return

    def _collect_and_persist_sync(self) -> dict[str, Any]:
        """Collect and durably publish one snapshot outside the event loop."""
        updated = self._collect_sync()
        self._persist_snapshot(updated)
        return updated

    def _collect_sync(self) -> dict[str, Any]:
        now = self.clock()
        previous = self._snapshot.get("repositories") if isinstance(self._snapshot, dict) else {}
        previous = previous if isinstance(previous, dict) else {}
        pb7_raw = str(pb7dir() or "").strip()
        pb8_raw = str(pb8dir() or "").strip()
        pb7_path = Path(pb7_raw).expanduser() if pb7_raw else None
        pb8_path = Path(pb8_raw).expanduser() if pb8_raw else None
        repositories = {
            "pbgui": self._collect_repository(
                "pbgui", self.pbgui_dir, PBGUI_UPSTREAM_URL, "main", previous.get("pbgui"), now
            ),
            "pb7": self._collect_repository(
                "pb7",
                pb7_path,
                PASSIVBOT_UPSTREAM_URL,
                "master",
                previous.get("pb7"),
                now,
            ),
            "pb8": self._collect_repository(
                "pb8",
                pb8_path,
                PASSIVBOT_UPSTREAM_URL,
                "master",
                previous.get("pb8"),
                now,
            ),
        }
        return {
            "schema_version": SCHEMA_VERSION,
            "source": "vps-monitor",
            "generated_at": now,
            "repositories": repositories,
        }

    def _collect_repository(
        self,
        name: str,
        repo_dir: Path | None,
        fallback_url: str,
        default_branch: str,
        previous: object,
        now: float,
    ) -> dict[str, Any]:
        previous_entry = dict(previous) if isinstance(previous, dict) else {}
        command = (
            ["git", "-C", str(repo_dir), "ls-remote", "--heads", "origin"]
            if self._is_repo(repo_dir)
            else ["git", "ls-remote", "--heads", fallback_url]
        )
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=20, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            return self._failed_entry(name, default_branch, previous_entry, now, type(exc).__name__)
        if result.returncode != 0:
            return self._failed_entry(name, default_branch, previous_entry, now, f"git exit {result.returncode}")
        heads: dict[str, str] = {}
        for line in str(result.stdout or "").splitlines()[:1000]:
            fields = line.strip().split()
            if len(fields) != 2 or not fields[1].startswith("refs/heads/"):
                continue
            commit = fields[0].lower()
            if len(commit) not in {40, 64} or any(char not in "0123456789abcdef" for char in commit):
                continue
            heads[fields[1][len("refs/heads/"):]] = commit
        target_commit = heads.get(default_branch, "")
        if not target_commit:
            return self._failed_entry(name, default_branch, previous_entry, now, "default branch missing")
        return {
            "name": name,
            "state": "ok",
            "default_branch": default_branch,
            "target_commit": target_commit,
            "heads": heads,
            "last_attempt_at": now,
            "last_success_at": now,
            "error": "",
        }

    @staticmethod
    def _failed_entry(
        name: str,
        default_branch: str,
        previous: dict[str, Any],
        now: float,
        error: str,
    ) -> dict[str, Any]:
        entry = dict(previous)
        has_last_known = bool(entry.get("target_commit"))
        entry.update({
            "name": name,
            "state": "stale" if has_last_known else "error",
            "default_branch": default_branch,
            "last_attempt_at": now,
            "error": str(error or "upstream unavailable")[:240],
        })
        entry.setdefault("target_commit", "")
        entry.setdefault("heads", {})
        entry.setdefault("last_success_at", 0.0)
        return entry

    def _load_snapshot(self) -> dict[str, Any]:
        try:
            if self.state_path.is_symlink() or not self.state_path.is_file():
                raise FileNotFoundError
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
                raise ValueError("unsupported snapshot")
            return payload
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            return {
                "schema_version": SCHEMA_VERSION,
                "source": "vps-monitor",
                "generated_at": 0.0,
                "repositories": {},
            }

    def _persist_snapshot(self, payload: dict[str, Any]) -> None:
        ensure_private_directory(self.state_path.parent)
        atomic_write_private_text(self.state_path, json.dumps(payload, indent=4, sort_keys=True) + "\n")

    @staticmethod
    def _is_repo(path: Path | None) -> bool:
        return bool(path is not None and (path / ".git").exists())
