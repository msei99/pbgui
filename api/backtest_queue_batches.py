"""Durable API-owned submission of PB8 backtest queue batches."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from secure_files import atomic_write_private_text, ensure_private_directory

SERVICE = "BacktestQueueBatches"


class BacktestQueueBatches:
    """Persist one browser command and submit its jobs after navigation."""

    def __init__(self, root: Path, submit_one: Callable[[dict[str, Any]], Any]) -> None:
        self.root = root
        self.submit_one = submit_one
        self._task: asyncio.Task | None = None
        self._running = False
        self._wake: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def _lock(self):
        ensure_private_directory(self.root)
        return advisory_file_lock(self.root / ".write")

    def _status_path(self, batch_id: str) -> Path:
        return self.root / batch_id / "status.json"

    def _read_status(self, batch_id: str) -> dict[str, Any] | None:
        path = self._status_path(batch_id)
        if not path.is_file() or path.is_symlink():
            return None
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return value if isinstance(value, dict) and value.get("batch_id") == batch_id else None

    def _statuses(self) -> list[dict[str, Any]]:
        if not self.root.is_dir() or self.root.is_symlink():
            return []
        result = []
        for directory in self.root.iterdir():
            if directory.is_dir() and not directory.is_symlink():
                try:
                    parsed = uuid.UUID(directory.name)
                except ValueError:
                    continue
                if str(parsed) == directory.name:
                    status = self._read_status(directory.name)
                    if status:
                        result.append(status)
        return sorted(result, key=lambda item: item.get("created_at", 0), reverse=True)

    def _write_status(self, status: dict[str, Any]) -> None:
        atomic_write_private_text(
            self._status_path(status["batch_id"]),
            json.dumps(status, indent=4) + "\n",
        )

    def list_for_owner(self, owner: str) -> list[dict[str, Any]]:
        """Return bounded status only, never saved configs."""
        with self._lock():
            return [
                {key: item.get(key) for key in ("batch_id", "status", "total", "confirmed", "error", "created_at")}
                for item in self._statuses() if item.get("owner") == owner
            ][:10]

    def submit(self, owner: str, items: list[dict[str, Any]]) -> dict[str, Any]:
        """Store a complete immutable batch before acknowledging the browser."""
        canonical = json.dumps(items, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        if len(canonical.encode("utf-8")) > 32 * 1024 * 1024:
            raise ValueError("Backtest batch exceeds 32 MB")
        fingerprint_items = json.loads(canonical)
        for item in fingerprint_items:
            item.pop("operation_id", None)
            pbgui = (item.get("config") or {}).get("pbgui")
            group = pbgui.get("backtest_result_group") if isinstance(pbgui, dict) else None
            if isinstance(group, dict):
                group.pop("id", None)
        fingerprint = hashlib.sha256(json.dumps(fingerprint_items, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        with self._lock():
            for status in self._statuses():
                if status.get("owner") != owner or status.get("fingerprint") != fingerprint:
                    continue
                if status["status"] == "complete" and time.time() - status["created_at"] > 300:
                    continue
                if status["status"] == "error":
                    status["status"] = "queued"
                    status["error"] = ""
                    self._write_status(status)
                self._signal()
                return self._public(status)
            batch_id = str(uuid.uuid4())
            directory = ensure_private_directory(self.root / batch_id)
            atomic_write_private_text(directory / "items.json", json.dumps(items, indent=4) + "\n")
            status = {
                "batch_id": batch_id, "owner": owner, "fingerprint": fingerprint,
                "status": "queued", "total": len(items), "confirmed": 0,
                "error": "", "created_at": time.time(),
            }
            self._write_status(status)
        self._signal()
        return self._public(status)

    def retry(self, owner: str, batch_id: str) -> dict[str, Any]:
        """Resume an errored batch from the first unconfirmed item."""
        with self._lock():
            status = self._read_status(batch_id)
            if not status or status.get("owner") != owner:
                raise KeyError(batch_id)
            if status["status"] == "error":
                status["status"] = "queued"
                status["error"] = ""
                self._write_status(status)
        self._signal()
        return self._public(status)

    @staticmethod
    def _public(status: dict[str, Any]) -> dict[str, Any]:
        return {key: status.get(key) for key in ("batch_id", "status", "total", "confirmed", "error", "created_at")}

    def _signal(self) -> None:
        if self._loop and self._wake:
            self._loop.call_soon_threadsafe(self._wake.set)

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._loop = asyncio.get_running_loop()
        self._wake = asyncio.Event()
        self._running = True
        self._task = self._loop.create_task(self._run(), name="backtest-v8-batch-submitter")

    async def stop(self) -> None:
        self._running = False
        self._signal()
        task = self._task
        if task:
            await asyncio.gather(task, return_exceptions=True)
        self._task = None
        self._loop = None
        self._wake = None

    def _next(self) -> dict[str, Any] | None:
        with self._lock():
            pending = [item for item in self._statuses() if item.get("status") in {"queued", "running"}]
            return min(pending, key=lambda item: item["created_at"]) if pending else None

    async def _run(self) -> None:
        while self._running:
            status = await asyncio.to_thread(self._next)
            if status:
                await self._process(status)
                continue
            assert self._wake is not None
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=2)
            except asyncio.TimeoutError:
                pass

    async def _process(self, status: dict[str, Any]) -> None:
        batch_id = status["batch_id"]
        try:
            items = json.loads((self.root / batch_id / "items.json").read_text(encoding="utf-8"))
            if not isinstance(items, list) or len(items) != status["total"]:
                raise ValueError("Batch payload is incomplete")
            status["status"] = "running"
            with self._lock():
                self._write_status(status)
            for index in range(status["confirmed"], status["total"]):
                if not self._running:
                    status["status"] = "queued"
                    break
                await asyncio.to_thread(self.submit_one, items[index])
                status["confirmed"] = index + 1
                with self._lock():
                    self._write_status(status)
            else:
                status["status"] = "complete"
        except Exception as exc:
            status["status"] = "error"
            status["error"] = f"Job {status['confirmed'] + 1} failed; retry the batch after checking the queue."
            _log(SERVICE, f"PB8 batch {batch_id} stopped at job {status['confirmed'] + 1}: {type(exc).__name__}", level="ERROR")
        finally:
            with self._lock():
                self._write_status(status)
            if status["status"] == "complete":
                try:
                    (self.root / batch_id / "items.json").unlink(missing_ok=True)
                except OSError as exc:
                    _log(SERVICE, f"Could not remove completed PB8 batch payload: {type(exc).__name__}", level="WARNING")
