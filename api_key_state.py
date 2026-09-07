from __future__ import annotations

import json
import os
import threading
import uuid
from copy import deepcopy
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR
from secure_files import atomic_write_private_text, ensure_private_directory, secure_private_file

SERVICE = "ApiKeyState"
RUNTIME_STATE_KEYS: frozenset[str] = frozenset({
    "hl_valid_until",
    "hl_credential_fingerprint",
    "bybit_expires_at",
    "bybit_ips",
    "bybit_credential_fingerprint",
})
_STATE_FILE = Path(PBGDIR) / "data" / "state" / "api_keys" / "api_key_state.json"
_LEGACY_STATE_FILE = Path(PBGDIR) / "data" / "api_key_state.json"
_TRANSACTION_FILE = _STATE_FILE.with_name("user_update_transaction.json")
_API_KEY_WRITE_TARGET = Path(PBGDIR) / "data" / "api-keys" / ".write"
_STATE_LOCK = threading.RLock()


class ApiKeyStateTransactionConflictError(RuntimeError):
    """Raised when an API-key state transaction cannot be resolved safely."""


@contextmanager
def _locked_state() -> Iterator[None]:
    """Serialize state access with API-key mutations across PBGui processes."""
    with advisory_file_lock(_API_KEY_WRITE_TARGET):
        with _STATE_LOCK:
            yield


def strip_runtime_extra(extra: dict[str, Any] | None) -> dict[str, Any]:
    """Remove runtime-managed expiry fields from user.extra payloads."""
    if not isinstance(extra, dict):
        return {}
    return {k: v for k, v in extra.items() if k not in RUNTIME_STATE_KEYS}


def _default_state() -> dict[str, Any]:
    return {"version": 1, "users": {}}


def _normalize_state(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return _default_state()
    raw_users = data.get("users", {})
    users: dict[str, dict[str, Any]] = {}
    if isinstance(raw_users, dict):
        for name, value in raw_users.items():
            if isinstance(name, str) and isinstance(value, dict):
                users[name] = dict(value)
    version = data.get("version", 1)
    if not isinstance(version, int):
        version = 1
    return {"version": version, "users": users}


def _load_state_unlocked() -> dict[str, Any]:
    state_path = _STATE_FILE if _STATE_FILE.exists() else _LEGACY_STATE_FILE
    if not state_path.exists():
        return _default_state()
    try:
        return _normalize_state(json.loads(state_path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        _log(SERVICE, f"Failed to read {state_path.name}: {exc}", level="WARNING")
        return _default_state()


def _write_state_unlocked(state: dict[str, Any]) -> None:
    ensure_private_directory(_STATE_FILE.parent)
    atomic_write_private_text(
        _STATE_FILE,
        json.dumps(_normalize_state(state), indent=4, sort_keys=True) + "\n",
    )


def _sync_parent(path: Path) -> None:
    """Durably publish a journal create or removal on POSIX filesystems."""
    if os.name != "posix":
        return
    descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _load_transaction_unlocked() -> dict[str, Any] | None:
    if not _TRANSACTION_FILE.exists():
        return None
    secure_private_file(_TRANSACTION_FILE)
    try:
        transaction = json.loads(_TRANSACTION_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ApiKeyStateTransactionConflictError("API-key update journal is unreadable") from exc
    required = {"id", "old_name", "new_name", "before_marker", "after_marker", "before", "target"}
    if not isinstance(transaction, dict) or transaction.get("version") != 1 or not required.issubset(transaction):
        raise ApiKeyStateTransactionConflictError("API-key update journal has an unsupported format")
    return transaction


def _write_transaction_unlocked(transaction: dict[str, Any]) -> None:
    ensure_private_directory(_TRANSACTION_FILE.parent)
    atomic_write_private_text(
        _TRANSACTION_FILE,
        json.dumps(transaction, indent=4, sort_keys=True) + "\n",
    )
    _sync_parent(_TRANSACTION_FILE)


def _remove_transaction_unlocked() -> None:
    _TRANSACTION_FILE.unlink(missing_ok=True)
    _sync_parent(_TRANSACTION_FILE)


def _selected_entries(users: dict[str, Any], old_name: str, new_name: str) -> dict[str, Any]:
    names = (old_name,) if old_name == new_name else (old_name, new_name)
    return {
        name: {"exists": name in users, "value": deepcopy(users.get(name))}
        for name in names
    }


def _apply_entries(users: dict[str, Any], entries: dict[str, Any]) -> None:
    for name, entry in entries.items():
        if entry.get("exists"):
            users[name] = deepcopy(entry.get("value"))
        else:
            users.pop(name, None)


def begin_user_state_transaction(
    old_name: str,
    new_name: str,
    *,
    before_marker: str,
    after_marker: str,
    clear_all: bool = False,
    clear_keys: tuple[str, ...] = (),
) -> str:
    """Persist an exact, secret-free runtime-state transition before credential replacement."""
    if not old_name or not new_name or not before_marker or not after_marker:
        raise ValueError("Incomplete API-key state transaction")
    with _locked_state():
        if _load_transaction_unlocked() is not None:
            raise ApiKeyStateTransactionConflictError("Another API-key update requires recovery")
        state = _load_state_unlocked()
        users = state.setdefault("users", {})
        before = _selected_entries(users, old_name, new_name)
        target_users = deepcopy(users)
        current = target_users.get(old_name, {})
        next_state = dict(current) if isinstance(current, dict) else {}
        if old_name != new_name:
            target_users.pop(old_name, None)
        if clear_all:
            next_state = {}
        else:
            for key in clear_keys:
                next_state.pop(key, None)
        if next_state:
            target_users[new_name] = next_state
        else:
            target_users.pop(new_name, None)
        transaction_id = uuid.uuid4().hex
        _write_transaction_unlocked({
            "version": 1,
            "id": transaction_id,
            "old_name": old_name,
            "new_name": new_name,
            "before_marker": before_marker,
            "after_marker": after_marker,
            "before": before,
            "target": _selected_entries(target_users, old_name, new_name),
        })
        return transaction_id


def get_pending_user_state_transaction() -> dict[str, Any] | None:
    """Return a detached pending journal for authoritative credential resolution."""
    with _locked_state():
        transaction = _load_transaction_unlocked()
        return deepcopy(transaction) if transaction is not None else None


def finish_user_state_transaction(transaction_id: str, *, commit: bool) -> None:
    """Apply the journal target or exact preimage and durably remove the journal."""
    with _locked_state():
        transaction = _load_transaction_unlocked()
        if transaction is None or transaction.get("id") != transaction_id:
            raise ApiKeyStateTransactionConflictError("API-key update journal changed")
        state = _load_state_unlocked()
        users = state.setdefault("users", {})
        old_name = str(transaction["old_name"])
        new_name = str(transaction["new_name"])
        current = _selected_entries(users, old_name, new_name)
        before = transaction["before"]
        target = transaction["target"]
        if current not in (before, target):
            raise ApiKeyStateTransactionConflictError("API-key runtime state changed during update")
        desired = target if commit else before
        if current != desired:
            _apply_entries(users, desired)
            _write_state_unlocked(state)
        _remove_transaction_unlocked()


def get_user_state(user_name: str) -> dict[str, Any]:
    if not user_name:
        return {}
    with _locked_state():
        users = _load_state_unlocked().get("users", {})
        state = users.get(user_name, {})
        return dict(state) if isinstance(state, dict) else {}


def update_user_state(user_name: str, **fields: Any) -> None:
    if not user_name:
        return
    with _locked_state():
        if _load_transaction_unlocked() is not None:
            raise ApiKeyStateTransactionConflictError("API-key update requires recovery")
        state = _load_state_unlocked()
        users = state.setdefault("users", {})
        current = dict(users.get(user_name, {})) if isinstance(users.get(user_name, {}), dict) else {}
        changed = False
        for key, value in fields.items():
            normalized = list(value) if isinstance(value, list) else value
            if normalized is None:
                if key in current:
                    current.pop(key, None)
                    changed = True
            elif current.get(key) != normalized:
                current[key] = normalized
                changed = True
        if current:
            if users.get(user_name) != current:
                users[user_name] = current
                changed = True
        elif user_name in users:
            users.pop(user_name, None)
            changed = True
        if changed:
            _write_state_unlocked(state)


def clear_user_state(user_name: str, keys: tuple[str, ...] | list[str] | None = None) -> None:
    if not user_name:
        return
    if not keys:
        delete_user_state(user_name)
        return
    update_user_state(user_name, **{key: None for key in keys})


def delete_user_state(user_name: str) -> None:
    if not user_name:
        return
    with _locked_state():
        if _load_transaction_unlocked() is not None:
            raise ApiKeyStateTransactionConflictError("API-key update requires recovery")
        state = _load_state_unlocked()
        users = state.setdefault("users", {})
        if user_name in users:
            users.pop(user_name, None)
            _write_state_unlocked(state)


def rename_user_state(old_name: str, new_name: str) -> None:
    if not old_name or not new_name or old_name == new_name:
        return
    with _locked_state():
        if _load_transaction_unlocked() is not None:
            raise ApiKeyStateTransactionConflictError("API-key update requires recovery")
        state = _load_state_unlocked()
        users = state.setdefault("users", {})
        old_state = users.get(old_name)
        if not isinstance(old_state, dict):
            return
        merged = dict(users.get(new_name, {})) if isinstance(users.get(new_name, {}), dict) else {}
        merged.update(old_state)
        users[new_name] = merged
        users.pop(old_name, None)
        _write_state_unlocked(state)
