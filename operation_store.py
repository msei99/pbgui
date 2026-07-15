"""Durable secret-free idempotency records for credential mutations."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Any

from file_lock import advisory_file_lock
from secure_files import (
    atomic_write_private_text,
    ensure_private_directory,
    ensure_private_directory_tree,
    secure_private_file,
)


SERVICE = "OperationStore"
_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class DurableOperationStore:
    """Persist operation ownership, progress, and final public responses."""

    def __init__(self, credential_root: Path | str) -> None:
        self.root = Path(os.path.abspath(Path(credential_root).expanduser()))
        self.path = self.root / "operations.json"
        self.lock_target = self.root / ".locks" / "operations"
        if self.root.is_symlink() or self.path.is_symlink() or self.lock_target.is_symlink():
            raise RuntimeError("Refusing symlinked durable operation path")
        ensure_private_directory(self.root)
        ensure_private_directory_tree(self.root, self.root / ".locks")

    def begin(self, operation_id: str, action: str, target: str = "") -> dict[str, Any]:
        """Create an operation intent or return its exact existing record."""

        operation_id = self._identifier(operation_id, "operation_id")
        action = self._identifier(action, "action")
        target = str(target)
        with advisory_file_lock(self.lock_target):
            state = self._read()
            current = state["operations"].get(operation_id)
            if isinstance(current, dict):
                if current.get("action") != action or current.get("target", "") != target:
                    raise ValueError("Operation ID was reused for another mutation")
                return deepcopy(current)
            record = {
                "operation_id": operation_id,
                "action": action,
                "target": target,
                "status": "pending",
                "stage": "started",
                "created_at": self._timestamp(),
                "updated_at": self._timestamp(),
            }
            state["operations"][operation_id] = record
            self._write(state)
            return deepcopy(record)

    def checkpoint(self, operation_id: str, stage: str, result: dict[str, Any] | None = None) -> dict[str, Any]:
        """Durably record a completed side-effect stage before continuing."""

        return self._update(operation_id, status="pending", stage=stage, result=result)

    def complete(self, operation_id: str, result: dict[str, Any]) -> dict[str, Any]:
        """Persist and return the exact secret-free final response."""

        return self._update(operation_id, status="complete", stage="complete", result=result)

    def get(self, operation_id: str) -> dict[str, Any] | None:
        """Return one durable operation record without mutating it."""

        operation_id = self._identifier(operation_id, "operation_id")
        with advisory_file_lock(self.lock_target):
            record = self._read()["operations"].get(operation_id)
            return deepcopy(record) if isinstance(record, dict) else None

    def _update(
        self,
        operation_id: str,
        *,
        status: str,
        stage: str,
        result: dict[str, Any] | None,
    ) -> dict[str, Any]:
        operation_id = self._identifier(operation_id, "operation_id")
        with advisory_file_lock(self.lock_target):
            state = self._read()
            record = state["operations"].get(operation_id)
            if not isinstance(record, dict):
                raise KeyError("Operation was not started")
            record = dict(record)
            record.update({"status": status, "stage": str(stage), "updated_at": self._timestamp()})
            if result is not None:
                record["result"] = deepcopy(result)
            state["operations"][operation_id] = record
            self._write(state)
            return deepcopy(record)

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"version": 1, "operations": {}}
        if self.path.is_symlink() or not self.path.is_file():
            raise ValueError("Durable operation store path is unsafe")
        secure_private_file(self.path)
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or payload.get("version") != 1 or not isinstance(payload.get("operations"), dict):
            raise ValueError("Unsupported durable operation store")
        return payload

    def _write(self, state: dict[str, Any]) -> None:
        atomic_write_private_text(self.path, json.dumps(state, indent=4, sort_keys=True) + "\n")

    @staticmethod
    def _identifier(value: str, label: str) -> str:
        text = str(value).strip()
        if not _ID_PATTERN.fullmatch(text):
            raise ValueError(f"{label} is invalid")
        return text

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()
