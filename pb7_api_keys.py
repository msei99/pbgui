"""Process-safe ownership and projection for PB7 ``api-keys.json``."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
from typing import Any

from credential_store import CredentialStore
from file_lock import advisory_file_lock
from secure_files import (
    atomic_write_private_bytes,
    atomic_write_private_text,
    ensure_private_directory,
    read_regular_file_nofollow,
    secure_private_file,
)


SERVICE = "PB7ApiKeys"

_STATUS_VERSION = 1
_TRADFI_KEY = "tradfi"


class PB7ApiKeysConflictError(RuntimeError):
    """Raised when an api-keys compare-and-swap generation is stale."""


class PB7ApiKeysMergeWriter:
    """Serialize owner-scoped updates to one PB7 API-key file."""

    def __init__(self, api_keys_path: Path | str, projection_status_path: Path | str) -> None:
        """Bind the writer to its PB7 file and secret-free retry status file."""

        self.api_keys_path = _lexical_absolute(api_keys_path)
        self.projection_status_path = _lexical_absolute(projection_status_path)
        self._approved_pb7_root = self.api_keys_path.parent
        self._lock_target = self.api_keys_path.with_name(f".{self.api_keys_path.name}.pbgui-merge")
        self._assert_safe_paths()

    def _locked(self):
        """Return the shared merge lock after lexical and symlink validation."""

        self._assert_safe_paths()
        return advisory_file_lock(self._lock_target)

    def _assert_safe_paths(self) -> None:
        """Reject a changed PB7 root, target, status path, or lock symlink."""

        try:
            self.api_keys_path.relative_to(self._approved_pb7_root)
        except ValueError as exc:
            raise RuntimeError("PB7 API-key path escaped its approved root") from exc
        for path in (
            self._approved_pb7_root,
            self.api_keys_path,
            self.projection_status_path,
            self._lock_target,
            self._lock_target.with_name(f"{self._lock_target.name}.lock"),
        ):
            _reject_symlink_components(path)

    def read(self) -> dict[str, Any]:
        """Return a locked copy of the complete PB7 API-key object."""

        with self._locked():
            return deepcopy(self._read_api_keys_unlocked())

    def read_bytes(self) -> bytes:
        """Return exact PB7 bytes after the same lexical and symlink checks."""

        with self._locked():
            self._assert_safe_paths()
            if not self.api_keys_path.exists():
                return b""
            secure_private_file(self.api_keys_path)
            return read_regular_file_nofollow(self.api_keys_path, self._approved_pb7_root)

    def write_exchange_payload(
        self,
        payload: dict[str, Any],
        *,
        expected_generation: int | None = None,
        backup_path: Path | str | None = None,
    ) -> dict[str, Any]:
        """Replace exchange-owned keys while preserving the reserved TradFi subtree."""

        if not isinstance(payload, dict):
            raise ValueError("Exchange API-key payload must be an object")
        exchange_payload = deepcopy(payload)
        exchange_payload.pop(_TRADFI_KEY, None)
        with self._locked():
            current = self._read_api_keys_unlocked()
            current_generation = _integer_generation(current.get("_api_serial"))
            if expected_generation is not None and current_generation != int(expected_generation):
                raise PB7ApiKeysConflictError(
                    f"Exchange API-key generation changed from {expected_generation} to {current_generation}"
                )
            if backup_path is not None and self.api_keys_path.exists():
                backup = _lexical_absolute(backup_path)
                _reject_symlink_components(backup)
                self._assert_safe_paths()
                atomic_write_private_bytes(
                    backup,
                    read_regular_file_nofollow(self.api_keys_path, self._approved_pb7_root),
                )
            if _TRADFI_KEY in current:
                exchange_payload[_TRADFI_KEY] = deepcopy(current[_TRADFI_KEY])
            self._write_api_keys_unlocked(exchange_payload)
            return deepcopy(exchange_payload)

    def project_tradfi(
        self,
        subtree: dict[str, Any] | None,
        *,
        source_fingerprint: str,
    ) -> dict[str, Any]:
        """Replace only the reserved TradFi subtree and persist retry state."""

        fingerprint = str(source_fingerprint or "").strip()
        if not fingerprint:
            raise ValueError("TradFi projection source fingerprint is required")
        if subtree is not None and not isinstance(subtree, dict):
            raise ValueError("TradFi projection must be an object or null")

        try:
            with self._locked():
                status = self._read_projection_status_unlocked()
                if status.get("source_fingerprint") == fingerprint:
                    desired_generation = max(1, _integer_generation(status.get("desired_generation")))
                else:
                    desired_generation = max(
                        _integer_generation(status.get("desired_generation")),
                        _integer_generation(status.get("applied_generation")),
                    ) + 1
                status.update({
                    "version": _STATUS_VERSION,
                    "status": "pending",
                    "source_fingerprint": fingerprint,
                    "desired_generation": desired_generation,
                    "attempts": _integer_generation(status.get("attempts")) + 1,
                    "last_attempt_at": _timestamp(),
                    "last_error": None,
                })
                self._write_projection_status_unlocked(status)

                current = self._read_api_keys_unlocked()
                current_tradfi = current.get(_TRADFI_KEY)
                current_projection_generation = (
                    _integer_generation(current_tradfi.get("_projection_generation"))
                    if isinstance(current_tradfi, dict)
                    else 0
                )
                applied_generation = _integer_generation(status.get("applied_generation"))
                if current_projection_generation > applied_generation:
                    current_fingerprint = (
                        str(current_tradfi.get("_source_fingerprint") or "")
                        if isinstance(current_tradfi, dict)
                        else ""
                    )
                    if current_fingerprint != fingerprint:
                        raise PB7ApiKeysConflictError(
                            "PB7 TradFi projection generation advanced outside this writer"
                        )
                    desired_generation = current_projection_generation

                projected = deepcopy(current)
                if subtree:
                    projected_subtree = deepcopy(subtree)
                    projected_subtree["_projection_generation"] = desired_generation
                    projected_subtree["_source_fingerprint"] = fingerprint
                    projected[_TRADFI_KEY] = projected_subtree
                else:
                    projected.pop(_TRADFI_KEY, None)
                self._write_api_keys_unlocked(projected)

                status.update({
                    "status": "current",
                    "desired_generation": desired_generation,
                    "applied_generation": desired_generation,
                    "applied_at": _timestamp(),
                    "last_error": None,
                })
                self._write_projection_status_unlocked(status)
                return _public_projection_status(status)
        except Exception as exc:
            self._record_projection_failure(fingerprint, exc)
            raise

    def projection_status(self) -> dict[str, Any]:
        """Return the persisted secret-free TradFi projection status."""

        with self._locked():
            return _public_projection_status(self._read_projection_status_unlocked())

    def project_store(self, store: CredentialStore) -> dict[str, Any]:
        """Build and project the latest vault state under the PB7 merge lock."""

        with self._locked():
            subtree, fingerprint = build_tradfi_projection(store)
            return self.project_tradfi(subtree, source_fingerprint=fingerprint)

    def restore_exchange_and_project(
        self,
        payload: dict[str, Any],
        store: CredentialStore,
        *,
        backup_path: Path | str | None = None,
    ) -> dict[str, Any]:
        """Restore exchange-owned content and reproject vault TradFi under one lock."""

        if not isinstance(payload, dict):
            raise ValueError("PB7 backup must contain a JSON object")
        with self._locked():
            current = self._read_api_keys_unlocked()
            merged = self.write_exchange_payload(
                payload,
                expected_generation=_integer_generation(current.get("_api_serial")),
                backup_path=backup_path,
            )
            subtree, fingerprint = build_tradfi_projection(store)
            projection = self.project_tradfi(subtree, source_fingerprint=fingerprint)
            return {"payload": merged, "projection": projection}

    def _record_projection_failure(self, fingerprint: str, exc: Exception) -> None:
        """Retain failed projection intent so a later materializer can retry it."""

        with self._locked():
            status = self._read_projection_status_unlocked()
            status.update({
                "version": _STATUS_VERSION,
                "status": "error",
                "source_fingerprint": fingerprint,
                "last_attempt_at": _timestamp(),
                "last_error": f"{type(exc).__name__}: {exc}",
            })
            self._write_projection_status_unlocked(status)

    def _read_api_keys_unlocked(self) -> dict[str, Any]:
        """Read and validate the complete PB7 API-key object while locked."""

        self._assert_safe_paths()
        if not self.api_keys_path.exists():
            return {}
        secure_private_file(self.api_keys_path)
        try:
            payload = json.loads(
                read_regular_file_nofollow(
                    self.api_keys_path,
                    self._approved_pb7_root,
                ).decode("utf-8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unable to read PB7 api-keys.json: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("PB7 api-keys.json must contain a JSON object")
        return payload

    def _write_api_keys_unlocked(self, payload: dict[str, Any]) -> None:
        """Atomically write and verify one owner-only PB7 API-key object."""

        self._assert_safe_paths()
        raw = (json.dumps(payload, indent=4) + "\n").encode("utf-8")
        atomic_write_private_bytes(self.api_keys_path, raw)
        self._assert_safe_paths()
        if self._read_api_keys_unlocked() != payload:
            raise RuntimeError("PB7 api-keys.json write verification failed")

    def _read_projection_status_unlocked(self) -> dict[str, Any]:
        """Read the secret-free retry state while the merge lock is held."""

        self._assert_safe_paths()
        if not self.projection_status_path.exists():
            return {
                "version": _STATUS_VERSION,
                "status": "never",
                "desired_generation": 0,
                "applied_generation": 0,
                "attempts": 0,
            }
        secure_private_file(self.projection_status_path)
        try:
            status = json.loads(self.projection_status_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unable to read TradFi projection status: {exc}") from exc
        if not isinstance(status, dict) or status.get("version") != _STATUS_VERSION:
            raise ValueError("Unsupported TradFi projection status format")
        return status

    def _write_projection_status_unlocked(self, status: dict[str, Any]) -> None:
        """Atomically persist the projection retry state with owner-only access."""

        self._assert_safe_paths()
        ensure_private_directory(self.projection_status_path.parent)
        atomic_write_private_text(
            self.projection_status_path,
            json.dumps(status, indent=4, sort_keys=True) + "\n",
        )


def build_tradfi_projection(
    store: CredentialStore,
    *,
    pending_profile_id: str | None = None,
) -> tuple[dict[str, Any] | None, str]:
    """Build a PB7-compatible subtree from active immutable vault generations."""

    records = store.list_tradfi(active_only=True)
    if pending_profile_id:
        pending = store.get_tradfi(pending_profile_id)
        if pending.get("active"):
            provider = str(pending.get("provider") or "")
            records = [record for record in records if str(record.get("provider") or "") != provider]
            records.append(pending)
    records = sorted(records, key=lambda item: str(item.get("id") or ""))
    source_records = [
        {
            "id": str(record["id"]),
            "provider": str(record.get("provider") or ""),
            "generation": int(record.get("generation") or 0),
            "active": bool(record.get("active", True)),
        }
        for record in records
    ]
    fingerprint = hashlib.sha256(
        json.dumps(source_records, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if not records:
        return None, fingerprint

    profiles: dict[str, dict[str, Any]] = {}
    for record in records:
        credential_id = str(record["id"])
        credentials = store.load_tradfi_credentials(credential_id)
        profiles[credential_id] = {
            "provider": str(record.get("provider") or ""),
            "label": str(record.get("label") or ""),
            "generation": int(record.get("generation") or 0),
            **credentials,
        }

    primary = records[0]
    primary_id = str(primary["id"])
    primary_profile = profiles[primary_id]
    subtree = {
        "active_profile_id": primary_id,
        "provider": str(primary.get("provider") or ""),
        "profiles": profiles,
    }
    for key, value in primary_profile.items():
        if key not in {"provider", "label", "generation"}:
            subtree[key] = value
    return subtree, fingerprint


def project_active_tradfi_profiles(
    store: CredentialStore,
    api_keys_path: Path | str,
    *,
    projection_status_path: Path | str | None = None,
    pending_profile_id: str | None = None,
) -> dict[str, Any]:
    """Project all active local TradFi profiles and return only safe status metadata."""

    status_path = Path(projection_status_path or (store.root / "pb7_projection.json"))
    writer = PB7ApiKeysMergeWriter(api_keys_path, status_path)
    if pending_profile_id is None:
        return writer.project_store(store)
    subtree, fingerprint = build_tradfi_projection(
        store,
        pending_profile_id=pending_profile_id,
    )
    return writer.project_tradfi(subtree, source_fingerprint=fingerprint)


def exchange_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a detached API-key payload without the reserved TradFi subtree."""

    result = deepcopy(payload) if isinstance(payload, dict) else {}
    result.pop(_TRADFI_KEY, None)
    return result


def _public_projection_status(status: dict[str, Any]) -> dict[str, Any]:
    """Return the stable non-secret subset used by APIs and materializers."""

    return {
        key: deepcopy(status.get(key))
        for key in (
            "status",
            "source_fingerprint",
            "desired_generation",
            "applied_generation",
            "attempts",
            "last_attempt_at",
            "applied_at",
            "last_error",
        )
        if key in status
    }


def _integer_generation(value: Any) -> int:
    """Normalize non-negative generation values from persisted JSON."""

    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _timestamp() -> str:
    """Return an explicit UTC timestamp for retry diagnostics."""

    return datetime.now(timezone.utc).isoformat()


def _lexical_absolute(path: Path | str) -> Path:
    """Return an absolute lexical path without resolving symlinks."""

    return Path(os.path.abspath(Path(path).expanduser()))


def _reject_symlink_components(path: Path) -> None:
    """Use lstat to reject every existing symlink component before file access."""

    candidate = _lexical_absolute(path)
    current = Path(candidate.anchor)
    for part in candidate.parts[1:]:
        current = current / part
        try:
            mode = os.lstat(current).st_mode
        except FileNotFoundError:
            break
        if stat.S_ISLNK(mode):
            raise RuntimeError(f"Refusing symlinked PB7 API-key path: {current}")
