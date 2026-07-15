"""Owner-only, process-safe storage for CMC and TradFi credentials."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import secrets
from typing import Any, Mapping

from file_lock import advisory_file_lock
from secure_files import (
    atomic_write_private_text,
    ensure_private_directory,
    ensure_private_directory_tree,
    secure_private_file,
)


SERVICE = "CredentialStore"

_ID_PATTERN = re.compile(r"^(cmc|tradfi)_[0-9a-f]{32}$")
_CATALOG_VERSION = 1
_OPERATION_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_PENDING_STAGES = frozenset({"stored", "published", "projected", "activated"})


class CredentialNotFoundError(KeyError):
    """Raised when a requested credential does not exist or was deleted."""


class CredentialStore:
    """Persist credential secrets separately from their metadata catalog."""

    def __init__(self, root: Path | str | None = None) -> None:
        """Initialize an owner-only store below the configured credentials root."""
        default_root = Path(__file__).resolve().parent / "data" / "credentials"
        self.root = Path(os.path.abspath(Path(root or default_root).expanduser()))
        self._catalog_path = self.root / "catalog.json"
        self._lock_target = self.root / ".locks" / "catalog"
        self._prepare_root()

    def create_cmc(
        self,
        api_key: str,
        *,
        label: str = "",
        active: bool = True,
        origin: str = "local",
        shared: bool = False,
        pending: bool = False,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a CMC credential with a stable random ID and generation one."""
        key = self._validate_secret(api_key, "CMC API key")
        operation_id = self._validate_operation_id(operation_id)
        with self._locked():
            catalog = self._read_catalog_unlocked()
            existing = self._record_for_operation_unlocked(catalog, "cmc", operation_id)
            if existing is not None:
                self._require_cmc_payload_unlocked(existing, key)
                self._require_operation_metadata(
                    existing,
                    {
                        "label": str(label).strip(),
                        "active": bool(active),
                        "origin": self._validate_origin(origin),
                        "shared": bool(shared),
                    },
                )
                return deepcopy(existing)
            credential_id = self._new_id("cmc", catalog["cmc"])
            now = self._timestamp()
            record = {
                "id": credential_id,
                "kind": "cmc",
                "label": str(label).strip(),
                "active": bool(active),
                "origin": self._validate_origin(origin),
                "shared": bool(shared),
                "generation": 1,
                "created_at": now,
                "updated_at": now,
            }
            self._set_pending_metadata(record, pending, operation_id)
            self._write_cmc_generation_unlocked(credential_id, 1, key)
            catalog["cmc"][credential_id] = record
            self._write_catalog_unlocked(catalog)
            return deepcopy(record)

    def update_cmc(
        self,
        credential_id: str,
        *,
        api_key: str | None = None,
        label: str | None = None,
        active: bool | None = None,
        origin: str | None = None,
        shared: bool | None = None,
        pending: bool | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        """Update CMC metadata and create a new immutable generation for a new key."""
        credential_id = self._validate_id(credential_id, "cmc")
        operation_id = self._validate_operation_id(operation_id)
        with self._locked():
            catalog = self._read_catalog_unlocked()
            record = self._live_record(catalog, "cmc", credential_id)
            if operation_id and operation_id in {
                record.get("pending_operation_id"),
                record.get("last_operation_id"),
            }:
                if api_key is not None:
                    self._require_cmc_payload_unlocked(
                        record,
                        self._validate_secret(api_key, "CMC API key"),
                    )
                self._require_operation_metadata(
                    record,
                    {
                        key: value
                        for key, value in {
                            "label": str(label).strip() if label is not None else None,
                            "active": bool(active) if active is not None else None,
                            "origin": self._validate_origin(origin) if origin is not None else None,
                            "shared": bool(shared) if shared is not None else None,
                        }.items()
                        if value is not None
                    },
                )
                return deepcopy(record)
            replacement = dict(record)
            if api_key is not None:
                generation = int(record["generation"]) + 1
                self._write_cmc_generation_unlocked(
                    credential_id,
                    generation,
                    self._validate_secret(api_key, "CMC API key"),
                )
                replacement["generation"] = generation
            if label is not None:
                replacement["label"] = str(label).strip()
            if active is not None:
                replacement["active"] = bool(active)
            if origin is not None:
                replacement["origin"] = self._validate_origin(origin)
            if shared is not None:
                replacement["shared"] = bool(shared)
            if pending is not None:
                self._set_pending_metadata(replacement, pending, operation_id)
            elif operation_id:
                replacement["last_operation_id"] = operation_id
            replacement["updated_at"] = self._timestamp()
            catalog["cmc"][credential_id] = replacement
            self._write_catalog_unlocked(catalog)
            return deepcopy(replacement)

    def get_cmc(self, credential_id: str) -> dict[str, Any]:
        """Return CMC metadata without exposing the API key."""
        credential_id = self._validate_id(credential_id, "cmc")
        with self._locked():
            return deepcopy(self._live_record(self._read_catalog_unlocked(), "cmc", credential_id))

    def list_cmc(self, *, active_only: bool = False) -> list[dict[str, Any]]:
        """Return the non-secret CMC catalog, excluding deleted records."""
        with self._locked():
            records = self._catalog_records(self._read_catalog_unlocked(), "cmc")
            if active_only:
                records = [record for record in records if self._is_selectable(record)]
            result = deepcopy(records)
            for record in result:
                if record.get("pending"):
                    record["active"] = False
            return result

    def load_cmc_key(self, credential_id: str, generation: int | None = None) -> str:
        """Load one CMC secret explicitly, optionally from an older generation."""
        credential_id = self._validate_id(credential_id, "cmc")
        with self._locked():
            record = self._live_record(self._read_catalog_unlocked(), "cmc", credential_id)
            selected_generation = int(record["generation"] if generation is None else generation)
            if selected_generation < 1 or selected_generation > int(record["generation"]):
                raise CredentialNotFoundError(f"Unknown CMC generation: {selected_generation}")
            payload = self._read_secret_unlocked(
                self._cmc_generation_path(credential_id, selected_generation)
            )
            key = payload.get("api_key")
            if not isinstance(key, str) or not key:
                raise ValueError("CMC generation does not contain a valid API key")
            return key

    def active_cmc_credentials(self) -> list[dict[str, Any]]:
        """Return active CMC records with keys for trusted local consumers."""
        with self._locked():
            catalog = self._read_catalog_unlocked()
            result = []
            for record in self._catalog_records(catalog, "cmc"):
                if not self._is_selectable(record):
                    continue
                payload = self._read_secret_unlocked(
                    self._cmc_generation_path(record["id"], int(record["generation"]))
                )
                key = payload.get("api_key")
                if not isinstance(key, str) or not key:
                    raise ValueError(f"CMC credential {record['id']} has no usable API key")
                item = deepcopy(record)
                item["api_key"] = key
                result.append(item)
            return result

    def delete_cmc(self, credential_id: str, *, operation_id: str | None = None) -> None:
        """Soft-delete a CMC credential while retaining immutable audit generations."""
        self._soft_delete(
            "cmc",
            self._validate_id(credential_id, "cmc"),
            operation_id=operation_id,
        )

    def create_tradfi(
        self,
        provider: str,
        credentials: Mapping[str, str],
        *,
        label: str = "",
        active: bool = True,
        origin: str = "local",
        shared: bool = False,
        pending: bool = False,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a TradFi provider credential without cataloging its secrets."""
        provider = self._validate_provider(provider)
        secret_values = self._validate_tradfi_credentials(credentials)
        operation_id = self._validate_operation_id(operation_id)
        with self._locked():
            catalog = self._read_catalog_unlocked()
            existing = self._record_for_operation_unlocked(catalog, "tradfi", operation_id)
            if existing is not None:
                self._require_tradfi_payload_unlocked(existing, secret_values)
                self._require_operation_metadata(
                    existing,
                    {
                        "provider": provider,
                        "label": str(label).strip(),
                        "active": bool(active),
                        "origin": self._validate_origin(origin),
                        "shared": bool(shared),
                    },
                )
                return deepcopy(existing)
            credential_id = self._new_id("tradfi", catalog["tradfi"])
            now = self._timestamp()
            record = {
                "id": credential_id,
                "kind": "tradfi",
                "provider": provider,
                "label": str(label).strip(),
                "active": bool(active),
                "origin": self._validate_origin(origin),
                "shared": bool(shared),
                "generation": 1,
                "created_at": now,
                "updated_at": now,
            }
            self._set_pending_metadata(record, pending, operation_id)
            self._write_tradfi_generation_unlocked(credential_id, 1, secret_values)
            catalog["tradfi"][credential_id] = record
            if record["active"] and not record.get("pending"):
                self._activate_tradfi_unlocked(catalog, credential_id)
            self._write_catalog_unlocked(catalog)
            return deepcopy(record)

    def update_tradfi(
        self,
        credential_id: str,
        *,
        provider: str | None = None,
        credentials: Mapping[str, str] | None = None,
        label: str | None = None,
        active: bool | None = None,
        origin: str | None = None,
        shared: bool | None = None,
        pending: bool | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        """Update a TradFi credential and version replacement secret values."""
        credential_id = self._validate_id(credential_id, "tradfi")
        operation_id = self._validate_operation_id(operation_id)
        with self._locked():
            catalog = self._read_catalog_unlocked()
            record = self._live_record(catalog, "tradfi", credential_id)
            if operation_id and operation_id in {
                record.get("pending_operation_id"),
                record.get("last_operation_id"),
            }:
                if credentials is not None:
                    self._require_tradfi_payload_unlocked(
                        record,
                        self._validate_tradfi_credentials(credentials),
                    )
                self._require_operation_metadata(
                    record,
                    {
                        key: value
                        for key, value in {
                            "provider": self._validate_provider(provider) if provider is not None else None,
                            "label": str(label).strip() if label is not None else None,
                            "active": bool(active) if active is not None else None,
                            "origin": self._validate_origin(origin) if origin is not None else None,
                            "shared": bool(shared) if shared is not None else None,
                        }.items()
                        if value is not None
                    },
                )
                return deepcopy(record)
            replacement = dict(record)
            if credentials is not None:
                generation = int(record["generation"]) + 1
                self._write_tradfi_generation_unlocked(
                    credential_id,
                    generation,
                    self._validate_tradfi_credentials(credentials),
                )
                replacement["generation"] = generation
            if provider is not None:
                replacement["provider"] = self._validate_provider(provider)
            if label is not None:
                replacement["label"] = str(label).strip()
            if active is not None:
                replacement["active"] = bool(active)
            if origin is not None:
                replacement["origin"] = self._validate_origin(origin)
            if shared is not None:
                replacement["shared"] = bool(shared)
            if pending is not None:
                self._set_pending_metadata(replacement, pending, operation_id)
            replacement["updated_at"] = self._timestamp()
            catalog["tradfi"][credential_id] = replacement
            self._remove_tradfi_selection_unlocked(catalog, credential_id)
            if replacement["active"] and not replacement.get("pending"):
                self._activate_tradfi_unlocked(catalog, credential_id)
            self._write_catalog_unlocked(catalog)
            return deepcopy(replacement)

    def get_tradfi(self, credential_id: str) -> dict[str, Any]:
        """Return TradFi metadata without exposing provider credentials."""
        credential_id = self._validate_id(credential_id, "tradfi")
        with self._locked():
            return deepcopy(
                self._live_record(self._read_catalog_unlocked(), "tradfi", credential_id)
            )

    def list_tradfi(self, *, active_only: bool = False) -> list[dict[str, Any]]:
        """Return the non-secret TradFi catalog, excluding deleted records."""
        with self._locked():
            catalog = self._read_catalog_unlocked()
            records = self._catalog_records(catalog, "tradfi")
            if active_only:
                selected = set(catalog["active_tradfi_profiles"].values())
                records = [
                    record
                    for record in records
                    if record["id"] in selected and self._is_selectable(record)
                ]
            result = deepcopy(records)
            for record in result:
                if record.get("pending"):
                    record["active"] = False
            return result

    def pending_stage(self, kind: str, credential_id: str, operation_id: str) -> str:
        """Return the durable stage for one exact pending API operation."""

        store_kind = self._validate_store_kind(kind)
        credential_id = self._validate_id(credential_id, store_kind)
        operation_id = self._validate_operation_id(operation_id) or ""
        with self._locked():
            record = self._live_record(self._read_catalog_unlocked(), store_kind, credential_id)
            if record.get("pending_operation_id") != operation_id:
                return "complete" if record.get("last_operation_id") == operation_id else "unknown"
            return str(record.get("pending_stage") or "stored")

    def set_pending_stage(
        self,
        kind: str,
        credential_id: str,
        operation_id: str,
        stage: str,
    ) -> dict[str, Any]:
        """Advance an exact pending operation after its external side effect succeeds."""

        store_kind = self._validate_store_kind(kind)
        credential_id = self._validate_id(credential_id, store_kind)
        operation_id = self._validate_operation_id(operation_id) or ""
        stage = str(stage)
        if stage not in _PENDING_STAGES:
            raise ValueError("Unknown credential mutation stage")
        with self._locked():
            catalog = self._read_catalog_unlocked()
            record = self._live_record(catalog, store_kind, credential_id)
            if record.get("pending_operation_id") != operation_id:
                if record.get("last_operation_id") == operation_id:
                    return deepcopy(record)
                raise RuntimeError("Credential pending operation changed")
            replacement = dict(record)
            replacement["pending_stage"] = stage
            replacement["updated_at"] = self._timestamp()
            catalog[store_kind][credential_id] = replacement
            self._write_catalog_unlocked(catalog)
            return deepcopy(replacement)

    def finalize_pending_mutation(
        self,
        kind: str,
        credential_id: str,
        operation_id: str,
    ) -> dict[str, Any]:
        """Make a fully published operation selectable and record retry completion."""

        store_kind = self._validate_store_kind(kind)
        credential_id = self._validate_id(credential_id, store_kind)
        operation_id = self._validate_operation_id(operation_id) or ""
        with self._locked():
            catalog = self._read_catalog_unlocked()
            record = self._live_record(catalog, store_kind, credential_id)
            if record.get("pending_operation_id") != operation_id:
                if record.get("last_operation_id") == operation_id:
                    return deepcopy(record)
                raise RuntimeError("Credential pending operation changed")
            replacement = dict(record)
            replacement.pop("pending", None)
            replacement.pop("pending_stage", None)
            replacement.pop("pending_operation_id", None)
            replacement["last_operation_id"] = operation_id
            replacement["updated_at"] = self._timestamp()
            catalog[store_kind][credential_id] = replacement
            if store_kind == "tradfi":
                self._remove_tradfi_selection_unlocked(catalog, credential_id)
                if replacement.get("active"):
                    self._activate_tradfi_unlocked(catalog, credential_id)
            self._write_catalog_unlocked(catalog)
            return deepcopy(replacement)

    def load_tradfi_credentials(
        self,
        credential_id: str,
        generation: int | None = None,
    ) -> dict[str, str]:
        """Load one TradFi generation explicitly for a trusted local consumer."""
        credential_id = self._validate_id(credential_id, "tradfi")
        with self._locked():
            record = self._live_record(self._read_catalog_unlocked(), "tradfi", credential_id)
            selected_generation = int(record["generation"] if generation is None else generation)
            if selected_generation < 1 or selected_generation > int(record["generation"]):
                raise CredentialNotFoundError(f"Unknown TradFi generation: {selected_generation}")
            payload = self._read_secret_unlocked(
                self._tradfi_generation_path(credential_id, selected_generation)
            )
            credentials = payload.get("credentials")
            return self._validate_tradfi_credentials(credentials)

    def delete_tradfi(self, credential_id: str) -> None:
        """Soft-delete a TradFi credential while retaining private generations."""
        self._soft_delete("tradfi", self._validate_id(credential_id, "tradfi"))

    def begin_tradfi_delete(self, credential_id: str, operation_id: str) -> dict[str, Any]:
        """Persist a retryable TradFi deletion intent before projection changes."""

        credential_id = self._validate_id(credential_id, "tradfi")
        operation_id = self._validate_operation_id(operation_id) or ""
        with self._locked():
            catalog = self._read_catalog_unlocked()
            record = self._live_record(catalog, "tradfi", credential_id)
            if operation_id in {record.get("pending_operation_id"), record.get("last_operation_id")}:
                return deepcopy(record)
            replacement = dict(record)
            replacement.update({
                "pending": True,
                "pending_delete": True,
                "pending_operation_id": operation_id,
                "pending_stage": "stored",
                "updated_at": self._timestamp(),
            })
            catalog["tradfi"][credential_id] = replacement
            self._remove_tradfi_selection_unlocked(catalog, credential_id)
            self._write_catalog_unlocked(catalog)
            return deepcopy(replacement)

    def finalize_tradfi_delete(self, credential_id: str, operation_id: str) -> dict[str, Any]:
        """Complete an exact pending delete only after projection and tombstone succeed."""

        credential_id = self._validate_id(credential_id, "tradfi")
        operation_id = self._validate_operation_id(operation_id) or ""
        with self._locked():
            catalog = self._read_catalog_unlocked()
            record = self._live_record(catalog, "tradfi", credential_id)
            if record.get("pending_operation_id") != operation_id or not record.get("pending_delete"):
                raise RuntimeError("TradFi deletion intent changed")
            replacement = dict(record)
            replacement.update({
                "active": False,
                "deleted_at": self._timestamp(),
                "updated_at": self._timestamp(),
                "last_operation_id": operation_id,
            })
            for field in ("pending", "pending_delete", "pending_operation_id", "pending_stage"):
                replacement.pop(field, None)
            catalog["tradfi"][credential_id] = replacement
            self._remove_tradfi_selection_unlocked(catalog, credential_id)
            self._write_catalog_unlocked(catalog)
            return deepcopy(replacement)

    def materialize_cluster_secret(
        self,
        credential_id: str,
        kind: str,
        generation: int,
        secret: str | Mapping[str, str],
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Idempotently materialize one fixed-ID Cluster Sync generation."""

        store_kind = "cmc" if kind == "cmc_api_key" else "tradfi" if kind == "tradfi_profile" else ""
        if not store_kind:
            raise ValueError(f"Unsupported cluster credential kind: {kind}")
        credential_id = self._validate_id(credential_id, store_kind)
        if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
            raise ValueError("Credential generation must be a positive integer")
        details = dict(metadata or {})
        if store_kind == "cmc":
            payload = {"api_key": self._validate_secret(secret, "CMC API key")}
            provider = None
        else:
            payload = {"credentials": self._validate_tradfi_credentials(secret)}
            provider = self._validate_provider(details.get("provider") or "unknown")

        with self._locked():
            catalog = self._read_catalog_unlocked()
            current = catalog[store_kind].get(credential_id)
            if current is not None and not isinstance(current, dict):
                raise ValueError("Credential catalog record must be an object")
            generation_path = (
                self._cmc_generation_path(credential_id, generation)
                if store_kind == "cmc"
                else self._tradfi_generation_path(credential_id, generation)
            )
            if generation_path.exists():
                if self._read_secret_unlocked(generation_path) != payload:
                    raise ValueError("Cluster credential generation already has different content")
            else:
                self._write_new_secret_unlocked(generation_path, payload)

            now = self._timestamp()
            record = dict(current or {})
            record.update({
                "id": credential_id,
                "kind": store_kind,
                "label": str(details.get("label") or record.get("label") or "").strip(),
                "active": (
                    bool(record.get("active", True))
                    if record.get("pending")
                    else bool(details.get("active", record.get("active", True)))
                ),
                "origin": "cluster",
                "shared": True,
                "generation": max(generation, int(record.get("generation") or 0)),
                "created_at": str(record.get("created_at") or now),
                "updated_at": now,
            })
            if provider is not None:
                record["provider"] = provider
            record.pop("deleted_at", None)
            catalog[store_kind][credential_id] = record
            if store_kind == "tradfi":
                self._remove_tradfi_selection_unlocked(catalog, credential_id)
                if record.get("active"):
                    self._activate_tradfi_unlocked(catalog, credential_id)
            self._write_catalog_unlocked(catalog)
            return deepcopy(record)

    def materialize_legacy_shadow(
        self,
        credential_id: str,
        kind: str,
        secret: str | Mapping[str, str],
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create or recover one fixed-ID local rolling-upgrade record."""

        store_kind = "cmc" if kind == "cmc_api_key" else "tradfi" if kind == "tradfi_profile" else ""
        if not store_kind:
            raise ValueError(f"Unsupported legacy shadow kind: {kind}")
        credential_id = self._validate_id(credential_id, store_kind)
        details = dict(metadata or {})
        payload = (
            {"api_key": self._validate_secret(secret, "CMC API key")}
            if store_kind == "cmc"
            else {"credentials": self._validate_tradfi_credentials(secret)}
        )
        provider = self._validate_provider(details.get("provider") or "unknown") if store_kind == "tradfi" else None
        with self._locked():
            catalog = self._read_catalog_unlocked()
            current = catalog[store_kind].get(credential_id)
            if current is not None and not isinstance(current, dict):
                raise ValueError("Credential catalog record must be an object")
            if isinstance(current, dict) and current.get("origin") != "legacy_shadow":
                raise ValueError("Credential ID is already cluster-managed")
            generation_path = (
                self._cmc_generation_path(credential_id, 1)
                if store_kind == "cmc"
                else self._tradfi_generation_path(credential_id, 1)
            )
            if generation_path.exists():
                if self._read_secret_unlocked(generation_path) != payload:
                    raise ValueError("Legacy shadow generation already has different content")
            else:
                self._write_new_secret_unlocked(generation_path, payload)
            now = self._timestamp()
            active = bool(details.get("active", True))
            record = dict(current or {})
            record.update({
                "id": credential_id,
                "kind": store_kind,
                "label": str(details.get("label") or record.get("label") or "").strip(),
                "active": active,
                "origin": "legacy_shadow",
                "shared": False,
                "generation": 1,
                "created_at": str(record.get("created_at") or now),
                "updated_at": now,
            })
            if provider is not None:
                record["provider"] = provider
            record.pop("deleted_at", None)
            catalog[store_kind][credential_id] = record
            if store_kind == "tradfi":
                if active:
                    self._activate_tradfi_unlocked(catalog, credential_id)
                else:
                    self._remove_tradfi_selection_unlocked(catalog, credential_id)
            self._write_catalog_unlocked(catalog)
            return deepcopy(record)

    def apply_cluster_tradfi_selection(self, active_by_provider: Mapping[str, str]) -> None:
        """Apply exact replicated active profile IDs without selecting pending intents."""

        selected = {
            self._validate_provider(provider): self._validate_id(profile_id, "tradfi")
            for provider, profile_id in active_by_provider.items()
            if profile_id
        }
        with self._locked():
            catalog = self._read_catalog_unlocked()
            for credential_id, record in catalog["tradfi"].items():
                if not isinstance(record, dict) or record.get("deleted_at") or record.get("pending"):
                    continue
                provider = self._validate_provider(record.get("provider") or "unknown")
                record["active"] = selected.get(provider) == credential_id
            catalog["active_tradfi_profiles"] = {
                provider: profile_id
                for provider, profile_id in selected.items()
                if isinstance(catalog["tradfi"].get(profile_id), dict)
                and not catalog["tradfi"][profile_id].get("deleted_at")
                and not catalog["tradfi"][profile_id].get("pending")
            }
            self._write_catalog_unlocked(catalog)

    def tombstone_cluster_secret(self, credential_id: str, kind: str) -> bool:
        """Soft-delete a materialized cluster secret, returning whether it existed."""

        store_kind = "cmc" if kind == "cmc_api_key" else "tradfi" if kind == "tradfi_profile" else ""
        if not store_kind:
            raise ValueError(f"Unsupported cluster credential kind: {kind}")
        credential_id = self._validate_id(credential_id, store_kind)
        with self._locked():
            catalog = self._read_catalog_unlocked()
            current = catalog[store_kind].get(credential_id)
            if not isinstance(current, dict) or current.get("deleted_at"):
                return False
            replacement = dict(current)
            replacement["active"] = False
            replacement["deleted_at"] = self._timestamp()
            replacement["updated_at"] = replacement["deleted_at"]
            catalog[store_kind][credential_id] = replacement
            self._write_catalog_unlocked(catalog)
            return True

    def _prepare_root(self) -> None:
        """Create and harden the store and lock directories."""
        self._reject_symlink(self.root)
        ensure_private_directory(self.root)
        ensure_private_directory_tree(self.root, self.root / ".locks")
        self._assert_store_path(self._catalog_path)
        self._assert_store_path(self._lock_target)
        self._reject_symlink(self._lock_target.with_name(f"{self._lock_target.name}.lock"))

    def _locked(self):
        """Return the store-wide advisory lock after revalidating boundaries."""
        self._prepare_root()
        return advisory_file_lock(self._lock_target)

    def _assert_store_path(self, path: Path) -> Path:
        """Validate that a path remains below the store and contains no symlink."""
        path = Path(os.path.abspath(path))
        try:
            relative = path.relative_to(self.root)
        except ValueError as exc:
            raise RuntimeError(f"Credential path is outside the configured root: {path}") from exc
        current = self.root
        self._reject_symlink(current)
        for part in relative.parts:
            if part in {"", ".", ".."}:
                raise RuntimeError(f"Invalid credential path component: {part!r}")
            current = current / part
            self._reject_symlink(current)
        return path

    @staticmethod
    def _reject_symlink(path: Path) -> None:
        """Reject symlinks at every credential filesystem boundary."""
        if path.is_symlink():
            raise RuntimeError(f"Refusing symlinked credential path: {path}")

    def _read_catalog_unlocked(self) -> dict[str, Any]:
        """Read and validate the metadata-only catalog while locked."""
        self._assert_store_path(self._catalog_path)
        if not self._catalog_path.exists():
            return {
                "version": _CATALOG_VERSION,
                "cmc": {},
                "tradfi": {},
                "active_tradfi_profiles": {},
            }
        secure_private_file(self._catalog_path)
        try:
            catalog = json.loads(self._catalog_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unable to read credential catalog: {exc}") from exc
        if not isinstance(catalog, dict) or catalog.get("version") != _CATALOG_VERSION:
            raise ValueError("Unsupported credential catalog format")
        if not isinstance(catalog.get("cmc"), dict) or not isinstance(catalog.get("tradfi"), dict):
            raise ValueError("Credential catalog collections must be objects")
        self._normalize_tradfi_activity_unlocked(catalog)
        return catalog

    def _write_catalog_unlocked(self, catalog: dict[str, Any]) -> None:
        """Atomically persist catalog metadata while locked."""
        self._assert_store_path(self._catalog_path)
        atomic_write_private_text(
            self._catalog_path,
            json.dumps(catalog, indent=4, sort_keys=True) + "\n",
        )

    def _write_cmc_generation_unlocked(
        self,
        credential_id: str,
        generation: int,
        api_key: str,
    ) -> None:
        """Create one CMC generation exactly once."""
        path = self._cmc_generation_path(credential_id, generation)
        self._write_new_secret_unlocked(path, {"api_key": api_key})

    def _write_tradfi_generation_unlocked(
        self,
        credential_id: str,
        generation: int,
        credentials: dict[str, str],
    ) -> None:
        """Create one private TradFi secret generation exactly once."""
        path = self._tradfi_generation_path(credential_id, generation)
        self._write_new_secret_unlocked(path, {"credentials": credentials})

    def _write_new_secret_unlocked(self, path: Path, payload: dict[str, Any]) -> None:
        """Write a generation without permitting overwrite or symlink replacement."""
        self._assert_store_path(path)
        ensure_private_directory_tree(self.root, path.parent)
        if path.exists() or path.is_symlink():
            if self._read_secret_unlocked(path) == payload:
                return
            raise RuntimeError("Credential generation already exists with different content")
        atomic_write_private_text(path, json.dumps(payload, indent=4, sort_keys=True) + "\n")

    def _read_secret_unlocked(self, path: Path) -> dict[str, Any]:
        """Read one private secret object after boundary and mode validation."""
        self._assert_store_path(path)
        if not path.is_file():
            raise CredentialNotFoundError(f"Credential secret does not exist: {path.name}")
        secure_private_file(path)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unable to read credential secret: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("Credential secret must contain a JSON object")
        return payload

    def _cmc_generation_path(self, credential_id: str, generation: int) -> Path:
        """Return a validated immutable CMC generation path."""
        credential_id = self._validate_id(credential_id, "cmc")
        if not isinstance(generation, int) or generation < 1:
            raise ValueError("Credential generation must be a positive integer")
        return self._assert_store_path(
            self.root / "cmc" / credential_id / f"generation-{generation}.json"
        )

    def _tradfi_generation_path(self, credential_id: str, generation: int) -> Path:
        """Return a validated TradFi generation path."""
        credential_id = self._validate_id(credential_id, "tradfi")
        if not isinstance(generation, int) or generation < 1:
            raise ValueError("Credential generation must be a positive integer")
        return self._assert_store_path(
            self.root / "tradfi" / credential_id / f"generation-{generation}.json"
        )

    def _soft_delete(
        self,
        kind: str,
        credential_id: str,
        *,
        operation_id: str | None = None,
    ) -> None:
        """Mark a credential deleted without rewriting immutable secret files."""
        operation_id = self._validate_operation_id(operation_id)
        with self._locked():
            catalog = self._read_catalog_unlocked()
            record = catalog[kind].get(credential_id)
            if not isinstance(record, dict):
                raise CredentialNotFoundError(f"Unknown {kind} credential: {credential_id}")
            if record.get("deleted_at"):
                if operation_id and record.get("last_operation_id") == operation_id:
                    return
                raise CredentialNotFoundError(f"Unknown {kind} credential: {credential_id}")
            replacement = dict(record)
            replacement["active"] = False
            replacement["deleted_at"] = self._timestamp()
            replacement["updated_at"] = replacement["deleted_at"]
            if operation_id:
                replacement["last_operation_id"] = operation_id
            catalog[kind][credential_id] = replacement
            if kind == "tradfi":
                self._remove_tradfi_selection_unlocked(catalog, credential_id)
            self._write_catalog_unlocked(catalog)

    def _normalize_tradfi_activity_unlocked(self, catalog: dict[str, Any]) -> None:
        """Materialize one deterministic explicit active profile ID per provider."""

        configured = catalog.get("active_tradfi_profiles")
        configured = dict(configured) if isinstance(configured, dict) else {}
        candidates: dict[str, list[str]] = {}
        for credential_id, record in catalog["tradfi"].items():
            if not isinstance(record, dict) or record.get("deleted_at") or record.get("pending"):
                continue
            if record.get("active"):
                provider = self._validate_provider(record.get("provider") or "unknown")
                candidates.setdefault(provider, []).append(str(credential_id))
        selected: dict[str, str] = {}
        for provider, ids in candidates.items():
            configured_id = str(configured.get(provider) or "")
            selected[provider] = configured_id if configured_id in ids else sorted(ids)[0]
        for credential_id, record in catalog["tradfi"].items():
            if not isinstance(record, dict) or record.get("deleted_at") or record.get("pending"):
                continue
            provider = str(record.get("provider") or "").strip().lower()
            record["active"] = selected.get(provider) == credential_id
        catalog["active_tradfi_profiles"] = selected

    def _activate_tradfi_unlocked(self, catalog: dict[str, Any], credential_id: str) -> None:
        """Select exactly one active record for a provider while holding the store lock."""

        selected = self._live_record(catalog, "tradfi", credential_id)
        provider = self._validate_provider(selected.get("provider") or "unknown")
        for other_id, record in catalog["tradfi"].items():
            if isinstance(record, dict) and not record.get("deleted_at"):
                if str(record.get("provider") or "").strip().lower() == provider:
                    record["active"] = other_id == credential_id
        catalog.setdefault("active_tradfi_profiles", {})[provider] = credential_id

    @staticmethod
    def _remove_tradfi_selection_unlocked(catalog: dict[str, Any], credential_id: str) -> None:
        active = catalog.setdefault("active_tradfi_profiles", {})
        for provider, selected_id in list(active.items()):
            if selected_id == credential_id:
                active.pop(provider, None)

    def _record_for_operation_unlocked(
        self,
        catalog: dict[str, Any],
        kind: str,
        operation_id: str | None,
    ) -> dict[str, Any] | None:
        """Resolve a create retry to its original record."""

        if not operation_id:
            return None
        matches = [
            record
            for record in catalog[kind].values()
            if isinstance(record, dict)
            and not record.get("deleted_at")
            and operation_id in {record.get("pending_operation_id"), record.get("last_operation_id")}
        ]
        if len(matches) > 1:
            raise RuntimeError("Credential operation ID is not unique")
        return matches[0] if matches else None

    def _require_cmc_payload_unlocked(self, record: dict[str, Any], api_key: str) -> None:
        payload = self._read_secret_unlocked(
            self._cmc_generation_path(str(record["id"]), int(record["generation"]))
        )
        if payload != {"api_key": api_key}:
            raise RuntimeError("Credential operation ID was reused with different content")

    def _require_tradfi_payload_unlocked(
        self,
        record: dict[str, Any],
        credentials: dict[str, str],
    ) -> None:
        payload = self._read_secret_unlocked(
            self._tradfi_generation_path(str(record["id"]), int(record["generation"]))
        )
        if payload != {"credentials": credentials}:
            raise RuntimeError("Credential operation ID was reused with different content")

    @staticmethod
    def _require_operation_metadata(record: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
        """Reject reuse of an operation ID for different non-secret intent."""

        if any(record.get(key) != value for key, value in expected.items()):
            raise RuntimeError("Credential operation ID was reused with different content")

    @staticmethod
    def _set_pending_metadata(
        record: dict[str, Any],
        pending: bool,
        operation_id: str | None,
    ) -> None:
        if pending:
            if not operation_id:
                raise ValueError("A pending credential mutation requires an operation ID")
            record["pending"] = True
            record["pending_operation_id"] = operation_id
            record["pending_stage"] = "stored"
        else:
            record.pop("pending", None)
            record.pop("pending_operation_id", None)
            record.pop("pending_stage", None)

    @staticmethod
    def _is_selectable(record: Mapping[str, Any]) -> bool:
        return bool(record.get("active")) and not bool(record.get("pending"))

    @staticmethod
    def _validate_store_kind(kind: str) -> str:
        value = str(kind).strip().lower()
        if value not in {"cmc", "tradfi"}:
            raise ValueError("Unsupported credential kind")
        return value

    @staticmethod
    def _validate_operation_id(operation_id: str | None) -> str | None:
        if operation_id is None:
            return None
        value = str(operation_id).strip()
        if not _OPERATION_PATTERN.fullmatch(value):
            raise ValueError("Credential operation ID is invalid")
        return value

    @staticmethod
    def _catalog_records(catalog: dict[str, Any], kind: str) -> list[dict[str, Any]]:
        """Return sorted live records from one catalog collection."""
        records = [record for record in catalog[kind].values() if not record.get("deleted_at")]
        return sorted(records, key=lambda record: (record.get("created_at", ""), record["id"]))

    @staticmethod
    def _live_record(
        catalog: dict[str, Any],
        kind: str,
        credential_id: str,
    ) -> dict[str, Any]:
        """Return a live catalog record or raise a consistent not-found error."""
        record = catalog[kind].get(credential_id)
        if not isinstance(record, dict) or record.get("deleted_at"):
            raise CredentialNotFoundError(f"Unknown {kind} credential: {credential_id}")
        return record

    @staticmethod
    def _new_id(kind: str, existing: Mapping[str, Any]) -> str:
        """Generate a stable random credential ID without exposing secret material."""
        while True:
            credential_id = f"{kind}_{secrets.token_hex(16)}"
            if credential_id not in existing:
                return credential_id

    @staticmethod
    def _validate_id(credential_id: str, kind: str) -> str:
        """Reject traversal and non-store identifiers at the public boundary."""
        value = str(credential_id)
        if not _ID_PATTERN.fullmatch(value) or not value.startswith(f"{kind}_"):
            raise ValueError(f"Invalid {kind} credential ID")
        return value

    @staticmethod
    def _validate_secret(value: str, description: str) -> str:
        """Require a non-empty string secret without transforming it."""
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{description} must be a non-empty string")
        return value

    @staticmethod
    def _validate_origin(value: str) -> str:
        """Validate a short non-secret origin label."""
        origin = str(value).strip().lower()
        if not origin or len(origin) > 64 or any(char in origin for char in "/\\\x00"):
            raise ValueError("Credential origin is invalid")
        return origin

    @staticmethod
    def _validate_provider(value: str) -> str:
        """Validate a provider identifier used only as metadata."""
        provider = str(value).strip().lower()
        if not provider or len(provider) > 64 or any(char in provider for char in "/\\\x00"):
            raise ValueError("TradFi provider is invalid")
        return provider

    @staticmethod
    def _validate_tradfi_credentials(credentials: Mapping[str, str] | Any) -> dict[str, str]:
        """Validate an opaque string-to-string TradFi secret mapping."""
        if not isinstance(credentials, Mapping) or not credentials:
            raise ValueError("TradFi credentials must be a non-empty mapping")
        result: dict[str, str] = {}
        for name, value in credentials.items():
            field = str(name).strip()
            if not field or len(field) > 128 or any(char in field for char in "/\\\x00"):
                raise ValueError("TradFi credential field is invalid")
            if not isinstance(value, str) or not value:
                raise ValueError(f"TradFi credential {field!r} must be a non-empty string")
            result[field] = value
        return result

    @staticmethod
    def _timestamp() -> str:
        """Return a stable UTC timestamp for metadata records."""
        return datetime.now(timezone.utc).isoformat()


def credential_mutation_lock(root: Path | str):
    """Serialize credential publication/projection transactions across API processes."""

    credential_root = Path(os.path.abspath(Path(root).expanduser()))
    ensure_private_directory(credential_root)
    ensure_private_directory_tree(credential_root, credential_root / ".locks")
    return advisory_file_lock(credential_root / ".locks" / "mutation")


def legacy_credential_writes_frozen(pbgdir: Path | str) -> bool:
    """Read the materialized writer-freeze flag without importing Cluster runtime code."""

    state_path = Path(os.path.abspath(Path(pbgdir).expanduser())) / "data" / "cluster" / "desired_state.json"
    if state_path.is_symlink() or not state_path.is_file():
        return False
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return True
    migration = payload.get("credential_migration") if isinstance(payload, dict) else None
    return isinstance(migration, dict) and migration.get("frozen") is True
