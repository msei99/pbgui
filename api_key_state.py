from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR

SERVICE = "ApiKeyState"
RUNTIME_STATE_KEYS: frozenset[str] = frozenset({"hl_valid_until", "bybit_expires_at", "bybit_ips"})
_STATE_FILE = Path(PBGDIR) / "data" / "api_key_state.json"
_STATE_LOCK = threading.Lock()


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
    if not _STATE_FILE.exists():
        return _default_state()
    try:
        return _normalize_state(json.loads(_STATE_FILE.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        _log(SERVICE, f"Failed to read { _STATE_FILE.name }: {exc}", level="WARNING")
        return _default_state()


def _write_state_unlocked(state: dict[str, Any]) -> None:
    _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _STATE_FILE.with_suffix(_STATE_FILE.suffix + ".tmp")
    try:
        tmp_path.write_text(
            json.dumps(_normalize_state(state), indent=4, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        tmp_path.replace(_STATE_FILE)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def get_user_state(user_name: str) -> dict[str, Any]:
    if not user_name:
        return {}
    with _STATE_LOCK:
        users = _load_state_unlocked().get("users", {})
        state = users.get(user_name, {})
        return dict(state) if isinstance(state, dict) else {}


def update_user_state(user_name: str, **fields: Any) -> None:
    if not user_name:
        return
    with _STATE_LOCK:
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
    with _STATE_LOCK:
        state = _load_state_unlocked()
        users = state.setdefault("users", {})
        if user_name in users:
            users.pop(user_name, None)
            _write_state_unlocked(state)


def rename_user_state(old_name: str, new_name: str) -> None:
    if not old_name or not new_name or old_name == new_name:
        return
    with _STATE_LOCK:
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