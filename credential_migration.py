"""Resumable migration of legacy CMC and TradFi credentials into the vault."""

from __future__ import annotations

import configparser
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
import platform
import psutil
import stat
import uuid
from typing import Any, Mapping

from cluster_credential_publisher import ClusterCredentialPublisher
from cluster_credentials import (
    SecretContext,
    canonical_json_bytes,
    deserialize_sealed_secret,
    ensure_node_key_material,
    seal_secret,
    serialize_sealed_secret,
)
from cluster_sync_command import _append_credential_migration_acks
from credential_store import CredentialStore
from file_lock import advisory_file_lock
from master.cluster_state import (
    CRYPTO_PUBLIC_FIELDS,
    ClusterPaths,
    ClusterStateError,
    append_operation,
    default_cluster_root,
    ensure_local_identity,
    load_operations,
    membership_signing_public_key,
    read_local_identity,
    rebuild_materialized_state,
)
from pb7_api_keys import (
    PB7ApiKeysMergeWriter,
    build_tradfi_projection,
    exchange_payload,
    project_active_tradfi_profiles,
)
from secure_files import (
    atomic_write_private_bytes,
    atomic_write_private_text,
    ensure_private_directory,
    ensure_private_directory_tree,
    read_regular_file_nofollow,
    secure_private_file,
)
from vps_inventory_store import inventory_file_lock


SERVICE = "CredentialMigration"
STATE_VERSION = 1
PHASES = (
    "protocol_barrier",
    "inventory",
    "import_publish",
    "materialization_confirmation",
    "credential_cutoff",
    "cleanup_confirmation",
    "legacy_cleanup",
    "scan",
    "unfreeze",
    "complete",
)

CMC_INI_FIELDS = frozenset({
    "api_key",
    "credit_limit_monthly",
    "credit_limit_monthly_reset",
    "credit_limit_monthly_reset_timestamp",
    "credits_used_day",
    "credits_used_month",
    "credits_left",
})
TRADFI_PROVIDERS = ("alpaca", "polygon", "finnhub", "alphavantage", "tiingo")
TRADFI_INI_SECRET_SUFFIXES = (
    "api_key",
    "api_secret",
    "api_key_id",
    "api_secret_key",
    "key",
    "secret",
    "secret_key",
    "api_token",
    "access_token",
    "token",
)
TRADFI_INI_FIELDS = frozenset(
    f"{provider}_{suffix}"
    for provider in TRADFI_PROVIDERS
    for suffix in TRADFI_INI_SECRET_SUFFIXES
)
LEGACY_VPS_FIELD = "coinmarketcap_api_key"


class CredentialMigrationError(RuntimeError):
    """Raised when migration state or a managed legacy source is unsafe."""


class CredentialMigrationBlocked(CredentialMigrationError):
    """Represent a recoverable barrier which must not prevent API startup."""


@dataclass
class _Source:
    """One legacy file generation plus transient credential values."""

    source_id: str
    source_key: str
    kind: str
    path: Path
    fingerprint: str
    generation: int
    items: list[dict[str, Any]]
    legacy_fields: list[str]

    def state_record(self, root: Path) -> dict[str, Any]:
        """Return a value-free source record suitable for persistent state."""

        try:
            relative_path = self.path.relative_to(root).as_posix()
        except ValueError:
            relative_path = str(self.path)
        return {
            "source_id": self.source_id,
            "source_key": self.source_key,
            "kind": self.kind,
            "path": relative_path,
            "fingerprint": self.fingerprint,
            "generation": self.generation,
            "legacy_fields": list(self.legacy_fields),
            "item_ids": [str(item["item_id"]) for item in self.items],
        }


class CredentialMigrationCoordinator:
    """Advance the credential cutover through durable, idempotent phases."""

    def __init__(
        self,
        pbgdir: Path | str | None = None,
        *,
        pb7_root: Path | str | None = None,
        credential_store: CredentialStore | None = None,
        cluster_root: Path | str | None = None,
    ) -> None:
        """Bind the coordinator to one PBGui installation and private state root."""

        self.root = Path(pbgdir or Path(__file__).resolve().parent).expanduser().resolve(strict=False)
        self.migration_root = self.root / "data" / "credentials" / "migration"
        self.state_path = self.migration_root / "state.json"
        self.backup_root = self.migration_root / "backups"
        self.cluster_root = Path(cluster_root or default_cluster_root(self.root)).resolve(strict=False)
        self.store = credential_store or CredentialStore(self.root / "data" / "credentials")
        self._configured_pb7_root = _lexical_absolute(pb7_root) if pb7_root else None
        self._transient_sources: dict[str, _Source] = {}
        ensure_private_directory_tree(self.migration_root, self.backup_root)

    def run(self) -> dict[str, Any]:
        """Advance all immediately runnable phases and return secret-free state."""

        with advisory_file_lock(self.state_path):
            state = self._read_state()
            for _ in range(len(PHASES) + 2):
                phase = str(state.get("phase") or "protocol_barrier")
                if phase == "complete":
                    try:
                        findings = self._scan_for_legacy_values(state)
                    except Exception as exc:
                        state["blocker_reason"] = _safe_migration_error(exc)
                        state["updated_at"] = _timestamp()
                        self._write_state(state)
                        return deepcopy(state)
                    if findings:
                        reason = "Legacy credential fields reappeared after migration: " + ", ".join(findings)
                        state.update({
                            "phase": "protocol_barrier",
                            "operation_id": uuid.uuid4().hex,
                            "sources": {},
                            "source_results": {},
                            "operation_ids": [],
                            "freeze_generation": 0,
                            "blocker_reason": reason,
                            "updated_at": _timestamp(),
                        })
                        self._write_state(state)
                        return deepcopy(state)
                    state["blocker_reason"] = None
                    state["status"] = "complete"
                    self._write_state(state)
                    return deepcopy(state)
                try:
                    next_phase = getattr(self, f"_phase_{phase}")(state)
                except CredentialMigrationBlocked as exc:
                    state["blocker_reason"] = str(exc)
                    state["status"] = (
                        "waiting_for_upgrade"
                        if state.get("outdated_node_ids") or state.get("outdated_services")
                        else "pending"
                    )
                    state["updated_at"] = _timestamp()
                    self._write_state(state)
                    return deepcopy(state)
                except Exception as exc:
                    state["blocker_reason"] = _safe_migration_error(exc)
                    state["status"] = "error"
                    state["updated_at"] = _timestamp()
                    self._write_state(state)
                    return deepcopy(state)
                state["phase"] = next_phase
                state["blocker_reason"] = None
                state["status"] = "complete" if next_phase == "complete" else "advancing"
                state["updated_at"] = _timestamp()
                if next_phase == "complete":
                    state["completed_at"] = state["updated_at"]
                self._write_state(state)
            raise CredentialMigrationError("credential migration phase loop did not converge")

    def _phase_inventory(self, state: dict[str, Any]) -> str:
        """Inventory locally only after every active writer acknowledged the freeze."""

        self._require_all_freeze_acks(state)
        state["phase"] = "inventory"
        state["updated_at"] = _timestamp()
        self._write_state(state)
        sources = self._inventory_sources()
        state.update({
            "version": STATE_VERSION,
            "phase": "inventory",
            "operation_id": str(state.get("operation_id") or uuid.uuid4().hex),
            "sources": {
                source_id: source.state_record(self.root)
                for source_id, source in sorted(sources.items())
            },
            "source_results": {
                source_id: {
                    "status": "inventoried",
                    "credentials": {},
                    "operation_ids": [],
                }
                for source_id in sorted(sources)
            },
            "operation_ids": [],
            "freeze_generation": int(state.get("freeze_generation") or 0),
            "blocker_reason": None,
            "started_at": state.get("started_at") or _timestamp(),
            "updated_at": _timestamp(),
        })
        self._write_state(state)
        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        local_node_id = str(read_local_identity(self.cluster_root)["node_id"])
        migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
        inventory_acks = migration.get("inventory_acks") if isinstance(migration.get("inventory_acks"), dict) else {}
        local_ack = inventory_acks.get(local_node_id) if isinstance(inventory_acks.get(local_node_id), dict) else {}
        source_fingerprints = {source.source_key: source.fingerprint for source in sources.values()}
        source_generations = {source.source_key: source.generation for source in sources.values()}
        if (
            int(local_ack.get("freeze_generation") or 0) != int(state["freeze_generation"])
            or local_ack.get("source_fingerprints", {}) != source_fingerprints
            or local_ack.get("source_generations", {}) != source_generations
        ):
            append_operation(
                self.cluster_root,
                "CREDENTIAL_INVENTORY_ACK",
                {
                    "freeze_generation": int(state["freeze_generation"]),
                    "node_id": local_node_id,
                    "source_fingerprints": source_fingerprints,
                    "source_generations": source_generations,
                },
            )
            materialized = rebuild_materialized_state(self.cluster_root, write=False)
        active_nodes = self._active_nodes(materialized)
        migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
        inventory_acks = migration.get("inventory_acks") if isinstance(migration.get("inventory_acks"), dict) else {}
        missing = [
            node_id for node_id in active_nodes
            if int((inventory_acks.get(node_id) or {}).get("freeze_generation") or 0)
            != int(state["freeze_generation"])
        ]
        if missing:
            raise CredentialMigrationBlocked(
                "Waiting for credential inventory ACK from active Cluster nodes: "
                + ", ".join(sorted(missing))
            )
        return "import_publish"

    def _phase_protocol_barrier(self, state: dict[str, Any]) -> str:
        """Publish the signed writer freeze and wait for every active v2 replica."""

        from credential_process_registry import process_barrier_readiness

        process_readiness = process_barrier_readiness(self.root)
        if not process_readiness["ready"]:
            state["outdated_services"] = process_readiness["waiting_services"]
            raise CredentialMigrationBlocked(
                "Local credential services are waiting for protocol v2: "
                + ", ".join(process_readiness["waiting_services"])
            )
        state.pop("outdated_services", None)
        self._ensure_local_cluster_identity_without_legacy_read()
        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        active_nodes = self._active_nodes(materialized)
        incompatible = [
            node_id
            for node_id, node in active_nodes.items()
            if not self._is_v2_crypto_node(node)
        ]
        if incompatible:
            state["outdated_node_ids"] = sorted(incompatible)
            raise CredentialMigrationBlocked(
                "Active Cluster nodes lack credential protocol v2 crypto: "
                + ", ".join(sorted(incompatible))
            )
        state.pop("outdated_node_ids", None)

        migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
        freeze_generation = int(state.get("freeze_generation") or 0)
        matching_freeze = (
            freeze_generation > 0
            and int(migration.get("freeze_generation") or 0) == freeze_generation
            and str(migration.get("migration_operation_id") or "") == str(state["operation_id"])
            and migration.get("frozen") is True
        )
        if not matching_freeze:
            freeze_generation = int(migration.get("freeze_generation") or 0) + 1
            operation = append_operation(
                self.cluster_root,
                "WRITER_FREEZE",
                {
                    "freeze_generation": freeze_generation,
                    "frozen": True,
                    "migration_operation_id": str(state["operation_id"]),
                },
            )
            state["freeze_generation"] = freeze_generation
            self._record_operation(state, operation)
            self._write_state(state)

        _append_credential_migration_acks(self.cluster_root, inventory_allowed=False)
        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        migration = materialized["desired_state"]["credential_migration"]
        acks = migration.get("freeze_acks") if isinstance(migration.get("freeze_acks"), dict) else {}
        missing = [
            node_id
            for node_id in active_nodes
            if not self._freeze_ack_matches(
                acks.get(node_id),
                freeze_generation,
            )
        ]
        if missing:
            raise CredentialMigrationBlocked(
                "Waiting for writer-freeze ACK from active Cluster nodes: "
                + ", ".join(sorted(missing))
            )
        return "inventory"

    def _require_all_freeze_acks(self, state: dict[str, Any]) -> None:
        """Require the current active writer set to freeze before any inventory read."""

        from credential_process_registry import process_barrier_readiness

        process_readiness = process_barrier_readiness(self.root)
        if not process_readiness["ready"]:
            state["outdated_services"] = process_readiness["waiting_services"]
            raise CredentialMigrationBlocked(
                "Local credential services are waiting for protocol v2: "
                + ", ".join(process_readiness["waiting_services"])
            )
        state.pop("outdated_services", None)
        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        active_nodes = self._active_nodes(materialized)
        migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
        acks = migration.get("freeze_acks") if isinstance(migration.get("freeze_acks"), dict) else {}
        missing = [
            node_id for node_id in active_nodes
            if not self._freeze_ack_matches(acks.get(node_id), int(state.get("freeze_generation") or 0))
        ]
        if missing:
            raise CredentialMigrationBlocked(
                "Waiting for writer-freeze ACK from active Cluster nodes: "
                + ", ".join(sorted(missing))
            )

    def _phase_import_publish(self, state: dict[str, Any]) -> str:
        """Import every unique value and idempotently publish its fixed generation."""

        sources = self._require_source_generations(state)
        publisher = ClusterCredentialPublisher(self.cluster_root, self.store)
        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        if _unresolved_migration_conflicts(materialized):
            raise CredentialMigrationBlocked("Migration credential conflict requires resolution")
        local_node_id = str(read_local_identity(self.cluster_root)["node_id"])

        for source_id, source in sorted(sources.items()):
            source_result = state.setdefault("source_results", {}).setdefault(
                source_id,
                {"status": "pending", "credentials": {}, "operation_ids": []},
            )
            source_result.setdefault("credentials", {})
            source_result.setdefault("operation_ids", [])
            for item in source.items:
                item_id = str(item["item_id"])
                recorded = source_result["credentials"].get(item_id)
                if isinstance(recorded, dict):
                    self._verify_and_publish_recorded(publisher, recorded)
                    continue
                kind = str(item["kind"])
                value = item["value"]
                candidate_id = _migration_candidate_id(
                    local_node_id,
                    int(state["freeze_generation"]),
                    source,
                    item_id,
                )
                credential_id, generation = _resolve_migration_import(
                    self,
                    materialized,
                    candidate_id=candidate_id,
                    kind=kind,
                    value=value,
                    provider=str(item.get("provider") or ""),
                    label=str(item.get("label") or "Imported legacy credential"),
                    freeze_generation=int(state["freeze_generation"]),
                )
                if not credential_id:
                    raise CredentialMigrationBlocked("Migration credential conflict requires resolution")
                if kind == "cmc_api_key":
                    if not self.store.get_cmc(credential_id).get("active"):
                        self.store.update_cmc(credential_id, active=True)
                elif not self.store.get_tradfi(credential_id).get("active"):
                    self.store.update_tradfi(credential_id, active=True)
                publication = (
                    publisher.publish_cmc(credential_id, generation, state="active")
                    if kind == "cmc_api_key"
                    else publisher.publish_tradfi(credential_id, generation)
                )
                if kind == "tradfi_profile":
                    activation = publisher.set_tradfi_active_profile(
                        str(item.get("provider") or "unknown"),
                        credential_id,
                    )
                    activation_id = str(activation.get("operation_id") or "")
                    if activation_id:
                        publication.setdefault("operation_ids", []).append(activation_id)
                record = {
                    "kind": kind,
                    "credential_id": credential_id,
                    "generation": generation,
                    "publication_operation_ids": list(publication.get("operation_ids") or []),
                    "status": "published",
                }
                source_result["credentials"][item_id] = record
                for operation_id in record["publication_operation_ids"]:
                    if operation_id not in state["operation_ids"]:
                        state["operation_ids"].append(operation_id)
                    if operation_id not in source_result["operation_ids"]:
                        source_result["operation_ids"].append(operation_id)
                source_result["status"] = "imported"
                self._write_state(state)
            source_result["status"] = "imported"
            self._write_state(state)
        return "materialization_confirmation"

    def _phase_materialization_confirmation(self, state: dict[str, Any]) -> str:
        """Require exact local and active-node generations before any deletion."""

        self._require_source_generations(state)
        local_ack = _append_credential_migration_acks(self.cluster_root)
        if local_ack.get("status") == "pending":
            raise CredentialMigrationBlocked(str(local_ack.get("reason") or "Local materialization pending"))
        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        active_nodes = self._active_nodes(materialized)
        desired = materialized["desired_state"]
        migration = desired.get("credential_migration") or {}
        acks = migration.get("materialization_acks")
        acks = acks if isinstance(acks, dict) else {}
        secrets = desired.get("secrets") if isinstance(desired.get("secrets"), dict) else {}
        missing: list[str] = []
        for node_id, node in active_nodes.items():
            role = str(node.get("role") or "")
            expected = {
                str(secret_id): int(secret.get("generation") or 0)
                for secret_id, secret in secrets.items()
                if isinstance(secret, dict)
                and (str(secret.get("audience") or "") == "cluster" or role == "master")
            }
            ack = acks.get(node_id) if isinstance(acks.get(node_id), dict) else {}
            if (
                int(ack.get("freeze_generation") or 0) != int(state["freeze_generation"])
                or ack.get("credential_generations", {}) != expected
            ):
                missing.append(node_id)
        if missing:
            raise CredentialMigrationBlocked(
                "Waiting for credential materialization ACK from active Cluster nodes: "
                + ", ".join(sorted(missing))
            )
        return "credential_cutoff"

    def _phase_credential_cutoff(self, state: dict[str, Any]) -> str:
        """Publish the signed protocol-v2 cutoff after an exchange-only checkpoint."""

        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        desired = materialized.get("desired_state") or {}
        migration = desired.get("credential_migration") or {}
        conflicts = migration.get("conflicts") if isinstance(migration.get("conflicts"), dict) else {}
        unresolved = [
            conflict_id for conflict_id, conflict in conflicts.items()
            if isinstance(conflict, dict) and str(conflict.get("status") or "") != "resolved"
        ]
        candidates = migration.get("candidates") if isinstance(migration.get("candidates"), dict) else {}
        acceptances = migration.get("candidate_acceptances") if isinstance(migration.get("candidate_acceptances"), dict) else {}
        pending_candidates = sorted(set(candidates) - set(acceptances))
        if unresolved or pending_candidates:
            raise CredentialMigrationBlocked(
                "Migration candidate resolution blocks credential cleanup"
            )
        self._checkpoint_legacy_api_keys_blob(state)
        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        desired = materialized.get("desired_state") or {}
        migration = desired.get("credential_migration") or {}
        current_cutoff = migration.get("cutoff") if isinstance(migration.get("cutoff"), dict) else {}
        checkpoint = state.get("legacy_api_keys_checkpoint") or {}
        current_hash = str(checkpoint.get("secret_blob_hash") or "")
        obsolete = sorted({
            str(operation.get("secret_blob_hash") or "")
            for operation in load_operations(self.cluster_root)
            if operation.get("op") == "UPSERT_API_KEYS"
            and operation.get("secret_blob_hash")
            and str(operation.get("secret_blob_hash")) != current_hash
        })
        cutoff_generation = int(current_cutoff.get("cutoff_generation") or 0)
        matching = (
            cutoff_generation > 0
            and current_cutoff.get("obsolete_secret_blob_hashes", []) == obsolete
            and int(current_cutoff.get("min_protocol") or 0) == 2
        )
        if not matching:
            operation = append_operation(
                self.cluster_root,
                "CREDENTIAL_CUTOFF",
                {
                    "cutoff_generation": cutoff_generation + 1,
                    "parent_generation": cutoff_generation,
                    "state_vector": dict(materialized.get("state_vector") or {}),
                    "min_protocol": 2,
                    "obsolete_secret_blob_hashes": obsolete,
                },
            )
            self._record_operation(state, operation)
            state["cutoff_generation"] = cutoff_generation + 1
            self._write_state(state)
            rebuild_materialized_state(self.cluster_root)
        else:
            state["cutoff_generation"] = cutoff_generation
        return "cleanup_confirmation"

    def _phase_cleanup_confirmation(self, state: dict[str, Any]) -> str:
        """Wait for every active replica to delete only cutoff-obsolete blobs."""

        local_ack = _append_credential_migration_acks(self.cluster_root)
        if local_ack.get("status") == "pending":
            raise CredentialMigrationBlocked(str(local_ack.get("reason") or "Local cutoff cleanup pending"))
        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        active_nodes = self._active_nodes(materialized)
        migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
        cutoff = migration.get("cutoff") if isinstance(migration.get("cutoff"), dict) else {}
        cleanup_acks = migration.get("cleanup_acks") if isinstance(migration.get("cleanup_acks"), dict) else {}
        cutoff_generation = int(cutoff.get("cutoff_generation") or 0)
        state_vector = dict(cutoff.get("state_vector") or {})
        missing = [
            node_id
            for node_id in active_nodes
            if not isinstance(cleanup_acks.get(node_id), dict)
            or int(cleanup_acks[node_id].get("cutoff_generation") or 0) != cutoff_generation
            or cleanup_acks[node_id].get("state_vector", {}) != state_vector
        ]
        if missing:
            raise CredentialMigrationBlocked(
                "Waiting for credential cutoff cleanup ACK from active Cluster nodes: "
                + ", ".join(sorted(missing))
            )
        return "legacy_cleanup"

    def _phase_legacy_cleanup(self, state: dict[str, Any]) -> str:
        """Back up and remove only unchanged legacy credential fields."""

        sources = self._require_source_generations(state, allow_cleaned=True)
        self._checkpoint_legacy_api_keys_blob(state)
        for source_id, source_record in sorted((state.get("sources") or {}).items()):
            result = state.setdefault("source_results", {}).setdefault(source_id, {})
            cleanup = result.setdefault("cleanup", {})
            if cleanup.get("status") == "cleaned":
                continue
            source = sources.get(source_id)
            path = self._source_path(source_record)
            with self._legacy_source_lock(str(source_record["kind"]), path):
                current_fingerprint = _file_fingerprint(path)
                if (
                    str(source_record["kind"]) == "pb7"
                    and cleanup.get("status") in {"backed_up", "prepared"}
                    and self._pb7_projection_is_current(path)
                ):
                    cleanup.update({
                        "status": "cleaned",
                        "expected_clean_fingerprint": current_fingerprint,
                        "cleaned_at": _timestamp(),
                    })
                    self._write_state(state)
                    continue
                if cleanup.get("expected_clean_fingerprint") == current_fingerprint:
                    cleanup["status"] = "cleaned"
                    self._write_state(state)
                    continue
                if current_fingerprint != str(source_record["fingerprint"]):
                    self._restart_inventory(
                        state,
                        f"Legacy source changed before cleanup: {source_record['path']}",
                    )
                backup_path = self._backup_source(state, source_id, path, source_record, cleanup)
                if str(source_record["kind"]) == "pb7":
                    self._cleanup_pb7(path, str(source_record["fingerprint"]))
                    cleanup["expected_clean_fingerprint"] = _file_fingerprint(path)
                else:
                    if source is None:
                        raise CredentialMigrationError(f"Legacy source disappeared: {source_record['path']}")
                    cleaned = self._cleaned_source_bytes(source)
                    cleanup.update({
                        "status": "prepared",
                        "backup": str(backup_path.relative_to(self.migration_root)),
                        "expected_clean_fingerprint": hashlib.sha256(cleaned).hexdigest(),
                    })
                    self._write_state(state)
                    if _file_fingerprint(path) != str(source_record["fingerprint"]):
                        self._restart_inventory(
                            state,
                            f"Legacy source changed during cleanup: {source_record['path']}",
                        )
                    atomic_write_private_bytes(path, cleaned)
                cleanup.update({
                    "status": "cleaned",
                    "backup": str(backup_path.relative_to(self.migration_root)),
                    "cleaned_at": _timestamp(),
                })
                self._write_state(state)
        return "scan"

    def _phase_scan(self, state: dict[str, Any]) -> str:
        """Block completion if a migrated value remains in any forbidden location."""

        findings = self._scan_for_legacy_values(state)
        state["scan"] = {
            "status": "blocked" if findings else "clean",
            "findings": findings,
            "scanned_at": _timestamp(),
        }
        if findings:
            raise CredentialMigrationBlocked(
                "Migrated credential sentinel remains in legacy fields: " + ", ".join(findings)
            )
        _append_credential_migration_acks(self.cluster_root)
        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        active_nodes = self._active_nodes(materialized)
        migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
        cutoff = migration.get("cutoff") if isinstance(migration.get("cutoff"), dict) else {}
        freeze_generation = int(migration.get("freeze_generation") or 0)
        cutoff_generation = int(cutoff.get("cutoff_generation") or 0)
        scan_acks = migration.get("scan_acks") if isinstance(migration.get("scan_acks"), dict) else {}
        blocked: list[str] = []
        missing: list[str] = []
        for node_id in active_nodes:
            ack = scan_acks.get(node_id) if isinstance(scan_acks.get(node_id), dict) else {}
            if (
                int(ack.get("freeze_generation") or 0) != freeze_generation
                or int(ack.get("cutoff_generation") or 0) != cutoff_generation
            ):
                missing.append(node_id)
            elif ack.get("status") != "clean":
                categories = ",".join(
                    str(item.get("path_category") or "unknown")
                    for item in ack.get("findings") or []
                    if isinstance(item, dict)
                )
                blocked.append(f"{node_id}:{categories or 'finding'}")
        if missing:
            raise CredentialMigrationBlocked(
                "Waiting for credential scan ACK from active Cluster nodes: "
                + ", ".join(sorted(missing))
            )
        if blocked:
            raise CredentialMigrationBlocked(
                "Credential scan findings block unfreeze: " + ", ".join(sorted(blocked))
            )
        return "unfreeze"

    def _phase_unfreeze(self, state: dict[str, Any]) -> str:
        """Publish completion only after cleanup ACKs and the allowlist scan pass."""

        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
        if migration.get("frozen") is not True:
            return "complete"
        operation = append_operation(
            self.cluster_root,
            "WRITER_FREEZE",
            {
                "freeze_generation": int(migration.get("freeze_generation") or 0) + 1,
                "frozen": False,
                "migration_operation_id": str(state["operation_id"]),
            },
        )
        self._record_operation(state, operation)
        rebuild_materialized_state(self.cluster_root)
        return "complete"

    def _inventory_sources(self) -> dict[str, _Source]:
        """Read every managed legacy source without persisting or logging values."""

        sources: dict[str, _Source] = {}
        ini_path = self.root / "pbgui.ini"
        if ini_path.is_file():
            if ini_path.is_symlink():
                raise CredentialMigrationError("Refusing symlinked pbgui.ini migration source")
            with advisory_file_lock(ini_path):
                raw = read_regular_file_nofollow(ini_path, self.root)
                source = self._inventory_ini(ini_path, raw)
            if source is not None:
                sources[source.source_id] = source
        inventory_root = self.root / "data" / "vpsmanager"
        if inventory_root.is_dir():
            for path in sorted(inventory_root.rglob("*.json")):
                if ".locks" in path.parts or path.is_symlink():
                    continue
                with inventory_file_lock(inventory_root, path):
                    raw = path.read_bytes()
                    source = self._inventory_vps_json(path, raw)
                if source is not None:
                    sources[source.source_id] = source
        pb7_root = self._pb7_root()
        if pb7_root is not None:
            path = pb7_root / "api-keys.json"
            if path.is_file():
                writer = PB7ApiKeysMergeWriter(path, self.store.root / "pb7_projection.json")
                raw = writer.read_bytes()
                source = self._inventory_pb7(path, raw)
                if source is not None:
                    sources[source.source_id] = source
        self._transient_sources = sources
        return sources

    def _inventory_ini(self, path: Path, raw: bytes) -> _Source | None:
        """Inventory legacy INI CMC usage and historical TradFi profiles."""

        parser = configparser.ConfigParser()
        try:
            parser.read_string(raw.decode("utf-8"))
        except (UnicodeDecodeError, configparser.Error) as exc:
            raise CredentialMigrationError("Unable to parse legacy INI source") from exc
        items: list[dict[str, Any]] = []
        fields: list[str] = []
        if parser.has_section("coinmarketcap"):
            present = sorted(field for field in CMC_INI_FIELDS if parser.has_option("coinmarketcap", field))
            fields.extend(f"coinmarketcap.{field}" for field in present)
            key = parser.get("coinmarketcap", "api_key", fallback="")
            if _usable_secret(key):
                items.append({
                    "item_id": "ini:coinmarketcap.api_key",
                    "kind": "cmc_api_key",
                    "value": key,
                    "label": "Imported pbgui.ini CoinMarketCap key",
                })
        if parser.has_section("tradfi_profiles"):
            present = sorted(field for field in TRADFI_INI_FIELDS if parser.has_option("tradfi_profiles", field))
            fields.extend(f"tradfi_profiles.{field}" for field in present)
            for provider in TRADFI_PROVIDERS:
                credentials: dict[str, str] = {}
                for suffix in TRADFI_INI_SECRET_SUFFIXES:
                    value = parser.get("tradfi_profiles", f"{provider}_{suffix}", fallback="")
                    if not _usable_secret(value):
                        continue
                    credential_field = (
                        "api_secret"
                        if suffix in {"api_secret", "api_secret_key", "secret", "secret_key"}
                        else "api_key"
                    )
                    credentials.setdefault(credential_field, value)
                if credentials:
                    items.append({
                        "item_id": f"ini:tradfi_profiles.{provider}",
                        "kind": "tradfi_profile",
                        "provider": provider,
                        "value": credentials,
                        "label": f"Imported INI {provider} profile",
                    })
        if not fields:
            return None
        return self._source(path, "ini", raw, items, fields)

    def _inventory_vps_json(self, path: Path, raw: bytes) -> _Source | None:
        """Inventory raw CMC fields from host, master, and pending JSON files."""

        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CredentialMigrationError(f"Unable to read legacy VPS JSON {path}: {exc}") from exc
        values = _json_field_values(payload, LEGACY_VPS_FIELD)
        if not values:
            return None
        items = [
            {
                "item_id": f"json:{self._relative_source_name(path)}:{location}",
                "kind": "cmc_api_key",
                "value": value,
                "label": f"Imported VPS inventory CMC key ({path.name})",
            }
            for location, value in values
            if _usable_secret(value)
        ]
        return self._source(
            path,
            "vps_json",
            raw,
            items,
            [f"{location}.{LEGACY_VPS_FIELD}" for location, _value in values],
        )

    def _inventory_pb7(self, path: Path, raw: bytes) -> _Source | None:
        """Inventory only pre-vault top-level PB7 TradFi credential formats."""

        secure_private_file(path)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CredentialMigrationError(f"Unable to read PB7 api-keys.json: {exc}") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("tradfi"), dict):
            return None
        tradfi = payload["tradfi"]
        if "_projection_generation" in tradfi and "_source_fingerprint" in tradfi:
            return None
        profiles: list[tuple[str, str, dict[str, str]]] = []
        nested = tradfi.get("profiles") if isinstance(tradfi.get("profiles"), dict) else None
        if nested:
            for profile_id, profile in sorted(nested.items()):
                if not isinstance(profile, dict):
                    continue
                provider = str(profile.get("provider") or tradfi.get("provider") or "unknown").strip().lower()
                credentials = _tradfi_secret_mapping(profile)
                if credentials:
                    profiles.append((str(profile_id), provider, credentials))
        else:
            for provider_name in sorted(TRADFI_PROVIDERS):
                provider_profile = tradfi.get(provider_name)
                if not isinstance(provider_profile, dict):
                    continue
                credentials = _tradfi_secret_mapping(provider_profile)
                if credentials:
                    profiles.append((provider_name, provider_name, credentials))
            provider = str(tradfi.get("provider") or "unknown").strip().lower()
            credentials = _tradfi_secret_mapping(tradfi)
            if credentials:
                profiles.append(("top-level", provider, credentials))
        if not profiles:
            return None
        items = [
            {
                "item_id": f"pb7:tradfi:{profile_id}",
                "kind": "tradfi_profile",
                "provider": provider,
                "value": credentials,
                "label": f"Imported PB7 {provider} profile",
            }
            for profile_id, provider, credentials in profiles
        ]
        return self._source(path, "pb7", raw, items, ["tradfi"])

    def _source(
        self,
        path: Path,
        kind: str,
        raw: bytes,
        items: list[dict[str, Any]],
        fields: list[str],
    ) -> _Source:
        """Build one exact source generation descriptor."""

        relative = self._relative_source_name(path)
        source_id = f"{kind}:{relative}"
        stat_result = path.stat()
        return _Source(
            source_id=source_id,
            source_key="source-" + hashlib.sha256(source_id.encode("utf-8")).hexdigest()[:24],
            kind=kind,
            path=path,
            fingerprint=hashlib.sha256(raw).hexdigest(),
            generation=int(stat_result.st_mtime_ns // 1_000_000),
            items=items,
            legacy_fields=fields,
        )

    def _require_source_generations(
        self,
        state: dict[str, Any],
        *,
        allow_cleaned: bool = False,
    ) -> dict[str, _Source]:
        """Re-inventory and require every uncleaned source fingerprint to match."""

        current = self._inventory_sources()
        expected = state.get("sources") if isinstance(state.get("sources"), dict) else {}
        for source_id, record in expected.items():
            cleanup = (((state.get("source_results") or {}).get(source_id) or {}).get("cleanup") or {})
            if allow_cleaned and cleanup.get("status") == "cleaned":
                continue
            source = current.get(source_id)
            if source is None or source.fingerprint != str(record.get("fingerprint") or ""):
                expected_clean = str(cleanup.get("expected_clean_fingerprint") or "")
                if allow_cleaned and expected_clean and _file_fingerprint(self._source_path(record)) == expected_clean:
                    continue
                if (
                    allow_cleaned
                    and str(record.get("kind") or "") == "pb7"
                    and cleanup.get("status") in {"backed_up", "prepared"}
                    and self._pb7_projection_is_current(self._source_path(record))
                ):
                    cleanup.update({
                        "status": "cleaned",
                        "expected_clean_fingerprint": _file_fingerprint(self._source_path(record)),
                        "cleaned_at": _timestamp(),
                    })
                    self._write_state(state)
                    continue
                self._restart_inventory(
                    state,
                    f"Legacy source generation changed: {record.get('path')}",
                )
        unexpected = sorted(set(current) - set(expected))
        if unexpected:
            self._restart_inventory(state, "New legacy credential source appeared during migration")
        return current

    def _restart_inventory(self, state: dict[str, Any], reason: str) -> None:
        """Persist a safe restart point without deleting any changed source."""

        state.update({
            "phase": "protocol_barrier",
            "operation_id": uuid.uuid4().hex,
            "sources": {},
            "source_results": {},
            "operation_ids": [],
            "freeze_generation": 0,
            "blocker_reason": reason,
            "updated_at": _timestamp(),
        })
        self._write_state(state)
        raise CredentialMigrationBlocked(reason)

    def _ensure_local_cluster_identity(self) -> None:
        """Initialize local identity and publish complete v2 crypto capability."""

        parser = self._read_ini_parser()
        configured_role = parser.get("main", "role", fallback="master").strip().lower()
        role = "vps" if configured_role in {"vps", "slave"} else "master"
        pbname = parser.get("main", "pbname", fallback="").strip() or platform.node()
        try:
            identity = read_local_identity(self.cluster_root)
        except Exception:
            identity = ensure_local_identity(self.cluster_root, role=role, pbname=pbname)
        _keys, materialized = ClusterCredentialPublisher(
            self.cluster_root,
            self.store,
        )._ensure_local_crypto_membership()
        node = ((materialized.get("cluster_nodes") or {}).get("nodes") or {}).get(str(identity["node_id"])) or {}
        if not self._is_v2_crypto_node(node):
            raise CredentialMigrationError("local Cluster v2 crypto capability could not be initialized")

    def _ensure_local_cluster_identity_without_legacy_read(self) -> None:
        """Initialize barrier identity without opening any legacy credential source."""

        try:
            identity = read_local_identity(self.cluster_root)
        except Exception:
            identity = ensure_local_identity(
                self.cluster_root,
                role="master",
                pbname=platform.node(),
            )
        _keys, materialized = ClusterCredentialPublisher(
            self.cluster_root,
            self.store,
        )._ensure_local_crypto_membership()
        node = ((materialized.get("cluster_nodes") or {}).get("nodes") or {}).get(str(identity["node_id"])) or {}
        if not self._is_v2_crypto_node(node):
            raise CredentialMigrationError("local Cluster v2 crypto capability could not be initialized")

    @staticmethod
    def _active_nodes(materialized: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Return active state replicas participating in credential barriers."""

        nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
        return {
            str(node_id): node
            for node_id, node in nodes.items()
            if isinstance(node, dict)
            and node.get("enabled", True) is not False
            and node.get("state_replica", True) is not False
            and str(node.get("role") or "") in {"master", "vps"}
        }

    @staticmethod
    def _is_v2_crypto_node(node: Mapping[str, Any]) -> bool:
        """Return whether one active member explicitly advertises complete v2 crypto."""

        try:
            protocol = int(node.get("credential_protocol_version") or 0)
        except (TypeError, ValueError):
            protocol = 0
        return (
            protocol >= 2
            and node.get("credential_capable") is True
            and all(node.get(field) for field in CRYPTO_PUBLIC_FIELDS)
        )

    @staticmethod
    def _freeze_ack_matches(
        ack: Any,
        generation: int,
    ) -> bool:
        """Return whether one node signed the exact active frozen barrier."""

        return (
            isinstance(ack, dict)
            and int(ack.get("freeze_generation") or 0) == generation
            and ack.get("frozen") is True
            and isinstance(ack.get("process_readiness"), list)
        )

    def _verify_and_publish_recorded(
        self,
        publisher: ClusterCredentialPublisher,
        record: dict[str, Any],
    ) -> None:
        """Verify a recorded vault generation and idempotently republish it."""

        credential_id = str(record["credential_id"])
        generation = int(record["generation"])
        if record["kind"] == "cmc_api_key":
            self.store.load_cmc_key(credential_id, generation)
            publication = publisher.publish_cmc(credential_id, generation, state="active")
        else:
            self.store.load_tradfi_credentials(credential_id)
            publication = publisher.publish_tradfi(credential_id, generation)
        record["publication_operation_ids"] = list(publication.get("operation_ids") or record.get("publication_operation_ids") or [])

    @staticmethod
    def _deterministic_credential_id(operation_id: str, item_id: str, kind: str) -> str:
        """Derive a random-looking stable ID from the persisted operation nonce."""

        prefix = "cmc" if kind == "cmc_api_key" else "tradfi"
        digest = hashlib.sha256(f"{operation_id}\0{item_id}\0{kind}".encode("utf-8")).hexdigest()
        return f"{prefix}_{digest[:32]}"

    def _backup_source(
        self,
        state: dict[str, Any],
        source_id: str,
        path: Path,
        source_record: dict[str, Any],
        cleanup: dict[str, Any],
    ) -> Path:
        """Create one immutable owner-only backup before source mutation."""

        operation_root = self.backup_root / str(state["operation_id"])
        ensure_private_directory_tree(self.migration_root, operation_root)
        filename = hashlib.sha256(source_id.encode("utf-8")).hexdigest()[:24] + ".bak"
        backup_path = operation_root / filename
        if not backup_path.exists():
            if _file_fingerprint(path) != str(source_record["fingerprint"]):
                self._restart_inventory(state, f"Legacy source changed before backup: {source_record['path']}")
            atomic_write_private_bytes(backup_path, path.read_bytes())
        cleanup.update({
            "status": "backed_up",
            "backup": str(backup_path.relative_to(self.migration_root)),
        })
        self._write_state(state)
        return backup_path

    def _cleaned_source_bytes(self, source: _Source) -> bytes:
        """Build the exact cleaned representation for one non-PB7 source."""

        if source.kind == "ini":
            parser = configparser.ConfigParser()
            try:
                parser.read_string(source.path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, configparser.Error) as exc:
                raise CredentialMigrationError("Unable to parse legacy INI source") from exc
            if parser.has_section("coinmarketcap"):
                for field in CMC_INI_FIELDS:
                    parser.remove_option("coinmarketcap", field)
            if parser.has_section("tradfi_profiles"):
                for field in TRADFI_INI_FIELDS:
                    parser.remove_option("tradfi_profiles", field)
                if not parser.items("tradfi_profiles"):
                    parser.remove_section("tradfi_profiles")
            buffer = io.StringIO()
            parser.write(buffer)
            return buffer.getvalue().encode("utf-8")
        payload = json.loads(source.path.read_text(encoding="utf-8"))
        _remove_json_field(payload, LEGACY_VPS_FIELD)
        return (json.dumps(payload, indent=4) + "\n").encode("utf-8")

    def _cleanup_pb7(self, path: Path, expected_fingerprint: str) -> None:
        """Replace legacy PB7 TradFi only through the vault projection writer."""

        writer = PB7ApiKeysMergeWriter(path, self.store.root / "pb7_projection.json")
        if hashlib.sha256(writer.read_bytes()).hexdigest() != expected_fingerprint:
            raise CredentialMigrationBlocked("PB7 api-keys.json changed before vault projection")
        project_active_tradfi_profiles(
            self.store,
            path,
            projection_status_path=self.store.root / "pb7_projection.json",
        )

    def _pb7_projection_is_current(self, path: Path) -> bool:
        """Return whether PB7 contains the exact current vault-owned projection."""

        try:
            payload = PB7ApiKeysMergeWriter(
                path,
                self.store.root / "pb7_projection.json",
            ).read()
            tradfi = payload.get("tradfi") if isinstance(payload, dict) else None
            return (
                isinstance(tradfi, dict)
                and str(tradfi.get("_source_fingerprint") or "") == build_tradfi_projection(self.store)[1]
                and int(tradfi.get("_projection_generation") or 0) > 0
            )
        except (OSError, ValueError):
            return False

    def _checkpoint_legacy_api_keys_blob(self, state: dict[str, Any]) -> None:
        """Advance desired API keys past legacy TradFi and prune obsolete local blobs."""

        paths = ClusterPaths.from_root(self.cluster_root)
        with advisory_file_lock(paths.root / ".append_sequence"):
            materialized = rebuild_materialized_state(self.cluster_root, write=False)
            desired = materialized.get("desired_state") if isinstance(materialized, dict) else {}
            api_keys = desired.get("api_keys") if isinstance(desired, dict) else None
            if not isinstance(api_keys, dict):
                state["legacy_api_keys_checkpoint"] = {"status": "not_present"}
                self._write_state(state)
                return
            current_hash = str(api_keys.get("secret_blob_hash") or "")
            raw = _read_content_blob(paths.secret_blobs, current_hash)
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CredentialMigrationError("Unable to read legacy API-key secret blob") from exc
            if not isinstance(payload, dict):
                raise CredentialMigrationError("Legacy API-key secret blob is not an object")

            operation: Mapping[str, Any] | None = None
            if "tradfi" in payload:
                cleaned = exchange_payload(payload)
                cleaned["_api_serial"] = max(
                    int(cleaned.get("_api_serial") or 0),
                    int(api_keys.get("serial") or 0),
                ) + 1
                secret_hash = _write_content_blob(
                    paths.secret_blobs,
                    json.dumps(cleaned, indent=4).encode("utf-8"),
                )
                redacted_hash = _write_content_blob(
                    paths.config_blobs,
                    json.dumps(_redact_legacy_api_keys(cleaned), sort_keys=True, separators=(",", ":")).encode("utf-8"),
                )
                operation = append_operation(
                    self.cluster_root,
                    "UPSERT_API_KEYS",
                    {
                        "api_serial": int(cleaned["_api_serial"]),
                        "payload_hash": redacted_hash,
                        "secret_blob_hash": secret_hash,
                        "sanitized": True,
                        "credential_protocol_version": 2,
                    },
                )
                current_hash = secret_hash
                self._record_operation(state, operation)
                rebuild_materialized_state(self.cluster_root)

            state["legacy_api_keys_checkpoint"] = {
                "status": "current",
                "operation_id": str((operation or {}).get("op_id") or ""),
                "secret_blob_hash": current_hash,
                "checkpointed_at": _timestamp(),
            }
            self._write_state(state)

    @staticmethod
    def _prune_legacy_api_key_blobs(blob_root: Path, current_hash: str) -> int:
        """Delete only obsolete local plaintext API-key blobs containing TradFi."""

        current_path = _content_blob_path(blob_root, current_hash)
        removed = 0
        if not blob_root.exists():
            return removed
        for path in sorted(blob_root.rglob("*.json")):
            _reject_symlink_components(path)
            if path == current_path or not path.is_file():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict) and "tradfi" in payload:
                path.unlink()
                removed += 1
        return removed

    def _scan_legacy_field_names(self) -> list[str]:
        """Cheap post-completion scan that never loads vault secrets or sentinel values."""

        findings: list[str] = []
        ini_path = self.root / "pbgui.ini"
        if ini_path.is_file():
            parser = self._read_ini_parser()
            for section, fields in (("coinmarketcap", CMC_INI_FIELDS), ("tradfi_profiles", TRADFI_INI_FIELDS)):
                for field in fields:
                    if parser.has_option(section, field):
                        findings.append(f"pbgui.ini:[{section}] {field}")
        inventory_root = self.root / "data" / "vpsmanager"
        if inventory_root.is_dir():
            for path in sorted(inventory_root.rglob("*.json")):
                if ".locks" in path.parts or path.is_symlink():
                    continue
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError):
                    continue
                if _json_field_values(payload, LEGACY_VPS_FIELD):
                    findings.append(f"{self._relative_source_name(path)}:{LEGACY_VPS_FIELD}")
        pb7_root = self._pb7_root()
        if pb7_root is not None:
            path = pb7_root / "api-keys.json"
            writer = PB7ApiKeysMergeWriter(path, self.store.root / "pb7_projection.json")
            payload = writer.read()
            tradfi = payload.get("tradfi") if isinstance(payload, dict) else None
            if isinstance(tradfi, dict) and not (
                "_projection_generation" in tradfi and "_source_fingerprint" in tradfi
            ):
                findings.append("PB7 api-keys.json:legacy tradfi")
        return sorted(set(findings))

    def _scan_for_legacy_values(self, state: dict[str, Any]) -> list[str]:
        """Scan forbidden fields for both legacy names and imported sentinel values."""

        sentinels = self._known_migration_sentinels(state)
        findings: list[str] = []
        ini_path = self.root / "pbgui.ini"
        if ini_path.is_file():
            parser = self._read_ini_parser()
            for section, fields in (("coinmarketcap", CMC_INI_FIELDS), ("tradfi_profiles", TRADFI_INI_FIELDS)):
                if parser.has_section(section):
                    for field in fields:
                        if parser.has_option(section, field):
                            findings.append(f"pbgui.ini:[{section}] {field}")
            text = ini_path.read_text(encoding="utf-8")
            if any(value in text for value in sentinels):
                findings.append("pbgui.ini:migrated-value")
        inventory_root = self.root / "data" / "vpsmanager"
        if inventory_root.is_dir():
            for path in sorted(inventory_root.rglob("*.json")):
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if _json_field_values(payload, LEGACY_VPS_FIELD):
                    findings.append(f"{self._relative_source_name(path)}:{LEGACY_VPS_FIELD}")
                text = json.dumps(payload)
                if any(value in text for value in sentinels):
                    findings.append(f"{self._relative_source_name(path)}:migrated-value")
        pb7_root = self._pb7_root()
        if pb7_root is not None and (pb7_root / "api-keys.json").is_file():
            try:
                payload = json.loads((pb7_root / "api-keys.json").read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                payload = {}
            tradfi = payload.get("tradfi") if isinstance(payload, dict) else None
            if isinstance(tradfi, dict) and not (
                "_projection_generation" in tradfi and "_source_fingerprint" in tradfi
            ):
                findings.append("PB7 api-keys.json:legacy tradfi")
        findings.extend(self._scan_managed_files(sentinels))
        findings.extend(self._scan_owned_process_argv(sentinels))
        return sorted(set(findings))

    def _known_migration_sentinels(self, state: Mapping[str, Any]) -> set[str]:
        """Retain migration plaintexts only in memory from approved private records."""

        references: dict[str, tuple[str, int]] = {}
        for result in (state.get("source_results") or {}).values():
            for record in ((result or {}).get("credentials") or {}).values():
                if not isinstance(record, Mapping):
                    continue
                credential_id = str(record.get("credential_id") or "")
                kind = str(record.get("kind") or "")
                generation = int(record.get("generation") or 0)
                if credential_id and generation > 0:
                    references[credential_id] = (kind, generation)

        try:
            materialized = rebuild_materialized_state(self.cluster_root, write=False)
        except ClusterStateError:
            materialized = {}
        migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
        candidates = migration.get("candidates") if isinstance(migration.get("candidates"), dict) else {}
        acceptances = migration.get("candidate_acceptances") if isinstance(migration.get("candidate_acceptances"), dict) else {}
        for candidate_id, acceptance in acceptances.items():
            if not isinstance(acceptance, Mapping):
                continue
            candidate = candidates.get(candidate_id) if isinstance(candidates.get(candidate_id), Mapping) else {}
            credential_id = str(acceptance.get("credential_id") or "")
            generation = int(acceptance.get("credential_generation") or 0)
            kind = str(candidate.get("candidate_kind") or (
                "cmc_api_key" if credential_id.startswith("cmc_")
                else "tradfi_profile" if credential_id.startswith("tradfi_")
                else ""
            ))
            if credential_id and generation > 0 and kind:
                references[credential_id] = (kind, max(generation, references.get(credential_id, (kind, 0))[1]))

        sentinels: set[str] = set()
        backup_needed = False
        for credential_id, (kind, accepted_generation) in references.items():
            try:
                record = (
                    self.store.get_cmc(credential_id)
                    if kind == "cmc_api_key"
                    else self.store.get_tradfi(credential_id)
                )
                for generation in range(1, max(accepted_generation, int(record["generation"])) + 1):
                    if kind == "cmc_api_key":
                        sentinels.add(self.store.load_cmc_key(credential_id, generation))
                    else:
                        sentinels.update(
                            self.store.load_tradfi_credentials(credential_id, generation).values()
                        )
            except (KeyError, ValueError):
                backup_needed = True

        for source in self._inventory_sources().values():
            for item in source.items:
                value = item.get("value")
                if isinstance(value, str) and value:
                    sentinels.add(value)
                elif isinstance(value, Mapping):
                    sentinels.update(str(secret) for secret in value.values() if secret)
        if backup_needed:
            sentinels.update(self._migration_backup_sentinels())
        return {value for value in sentinels if value}

    def _migration_backup_sentinels(self) -> set[str]:
        """Read owner-only migration backups only when a local generation is unavailable."""

        sentinels: set[str] = set()
        if not self.backup_root.is_dir() or self.backup_root.is_symlink():
            return sentinels
        for path in sorted(self.backup_root.rglob("*.bak")):
            if path.is_symlink() or not path.is_file():
                continue
            secure_private_file(path)
            sentinels.update(_legacy_backup_values(path.read_bytes()))
        return sentinels

    def _scan_managed_files(self, sentinels: set[str]) -> list[str]:
        """Scan only PBGui-managed logs, task metadata, and Cluster blobs."""

        findings: list[str] = []
        encoded = [value.encode("utf-8") for value in sentinels]
        roots = (
            (self.root / "data" / "logs", "managed-log", None),
            (self.root / "data" / "ohlcv" / "_tasks", "task-metadata", {".json"}),
            (self.root / "data" / "bt_v7_queue", "task-metadata", {".json"}),
            (self.root / "data" / "opt_v7_queue", "task-metadata", {".json"}),
            (self.cluster_root / "config_blobs", "cluster-blob", None),
            (self.cluster_root / "secret_blobs", "cluster-blob", None),
        )
        for managed_root, label, suffixes in roots:
            if not managed_root.is_dir() or managed_root.is_symlink():
                continue
            for path in sorted(managed_root.rglob("*")):
                if path.is_symlink() or not path.is_file():
                    continue
                if _path_is_below(path, self.store.root) or _path_is_below(path, self.backup_root):
                    continue
                if suffixes is not None and path.suffix.lower() not in suffixes:
                    continue
                if label == "managed-log":
                    if _file_contains_any(path, encoded):
                        findings.append(f"{self._relative_source_name(path)}:{label}")
                    continue
                try:
                    raw = path.read_bytes()
                except OSError:
                    continue
                relative = self._relative_source_name(path)
                if label == "task-metadata" and path.suffix.lower() == ".json":
                    try:
                        payload = json.loads(raw.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        payload = None
                    if payload is not None:
                        findings.extend(
                            f"{relative}:{field}"
                            for field in _json_secret_locations(payload, sentinels)
                        )
                        continue
                if any(value in raw for value in encoded):
                    findings.append(f"{relative}:{label}")
        return findings

    def _scan_owned_process_argv(self, sentinels: set[str]) -> list[str]:
        """Inspect argv only for current-user processes tied to this PBGui checkout."""

        findings: list[str] = []
        current_uid = os.getuid()
        for process in psutil.process_iter(["pid", "uids", "cmdline"]):
            try:
                uids = process.info.get("uids")
                argv = process.info.get("cmdline") or []
                if uids is None or int(uids.real) != current_uid or not argv:
                    continue
                owned = any(_argv_path_is_below(arg, self.root) for arg in argv)
                if not owned:
                    cwd = _lexical_absolute(process.cwd())
                    owned = (cwd == self.root or self.root in cwd.parents) and any(
                        str(argument).endswith(".py")
                        and _argv_path_is_below(cwd / str(argument), self.root)
                        for argument in argv
                    )
                if not owned:
                    continue
                for index, argument in enumerate(argv):
                    if any(value in str(argument) for value in sentinels):
                        findings.append(f"process:{int(process.info['pid'])}:argv[{index}]")
            except (psutil.Error, OSError, ValueError):
                continue
        return findings

    def _pb7_root(self) -> Path | None:
        """Return the configured PB7 root without importing runtime config modules."""

        if self._configured_pb7_root is not None:
            return self._configured_pb7_root
        parser = self._read_ini_parser()
        value = parser.get("main", "pb7dir", fallback="").strip()
        return _lexical_absolute(value) if value else None

    def _read_ini_parser(self) -> configparser.ConfigParser:
        """Read the local INI if present into an isolated parser."""

        parser = configparser.ConfigParser()
        path = self.root / "pbgui.ini"
        if path.is_file():
            try:
                with path.open("r", encoding="utf-8") as handle:
                    parser.read_file(handle)
            except (OSError, UnicodeError, configparser.Error) as exc:
                raise CredentialMigrationError("Unable to parse local INI") from exc
        return parser

    def _source_path(self, record: Mapping[str, Any]) -> Path:
        """Resolve a persisted source path below PBGui or the configured PB7 root."""

        raw = Path(str(record.get("path") or ""))
        path = raw if raw.is_absolute() else self.root / raw
        path = _lexical_absolute(path)
        allowed = [self.root]
        pb7_root = self._pb7_root()
        if pb7_root is not None:
            allowed.append(pb7_root)
        if not any(path == _lexical_absolute(root) or _lexical_absolute(root) in path.parents for root in allowed):
            raise CredentialMigrationError(f"Migration source escapes approved roots: {path}")
        _reject_symlink_components(path)
        return path

    def _legacy_source_lock(self, kind: str, path: Path):
        """Return the source owner's cross-process lock for backup and cleanup."""

        if kind == "ini":
            return advisory_file_lock(path)
        if kind == "vps_json":
            return inventory_file_lock(self.root / "data" / "vpsmanager", path)
        if kind == "pb7":
            writer = PB7ApiKeysMergeWriter(path, self.store.root / "pb7_projection.json")
            return writer._locked()
        raise CredentialMigrationError(f"Unsupported legacy source kind: {kind}")

    def _relative_source_name(self, path: Path) -> str:
        """Return a stable non-secret display path for migration metadata."""

        try:
            return _lexical_absolute(path).relative_to(self.root).as_posix()
        except ValueError:
            return f"pb7/{path.name}"

    def _read_state(self) -> dict[str, Any]:
        """Read and validate owner-only migration state."""

        if not self.state_path.exists():
            return {
                "version": STATE_VERSION,
                "phase": "protocol_barrier",
                "operation_id": uuid.uuid4().hex,
                "sources": {},
                "source_results": {},
                "operation_ids": [],
                "blocker_reason": None,
                "started_at": _timestamp(),
            }
        secure_private_file(self.state_path)
        try:
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CredentialMigrationError(f"Unable to read credential migration state: {exc}") from exc
        if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
            raise CredentialMigrationError("Unsupported credential migration state")
        if state.get("phase") not in PHASES:
            raise CredentialMigrationError("Credential migration state has an invalid phase")
        return state

    def _write_state(self, state: dict[str, Any]) -> None:
        """Atomically persist secret-free migration progress."""

        ensure_private_directory(self.migration_root)
        atomic_write_private_text(
            self.state_path,
            json.dumps(state, indent=4, sort_keys=True) + "\n",
        )

    @staticmethod
    def _record_operation(state: dict[str, Any], operation: Mapping[str, Any]) -> None:
        """Record one operation ID exactly once in local migration state."""

        operation_id = str(operation.get("op_id") or "")
        if operation_id and operation_id not in state.setdefault("operation_ids", []):
            state["operation_ids"].append(operation_id)


def run_credential_migration(pbgdir: Path | str | None = None) -> dict[str, Any]:
    """Run or resume the local credential migration coordinator."""

    coordinator = CredentialMigrationCoordinator(pbgdir)
    materialized = rebuild_materialized_state(coordinator.cluster_root, write=False)
    local_node_id = str(read_local_identity(coordinator.cluster_root)["node_id"])
    nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
    master_node_ids = sorted(
        str(node_id)
        for node_id, node in nodes.items()
        if isinstance(node, dict)
        and node.get("enabled", True) is not False
        and node.get("state_replica", True) is not False
        and str(node.get("role") or "") == "master"
    )
    elected_node_id = master_node_ids[0] if master_node_ids else local_node_id
    if local_node_id != elected_node_id:
        migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
        return {
            "version": STATE_VERSION,
            "phase": "protocol_barrier",
            "status": "not_coordinator",
            "coordinator_node_id": elected_node_id,
            "freeze_generation": int(migration.get("freeze_generation") or 0),
        }

    from credential_rolling_bootstrap import bootstrap_local_legacy_credentials

    bootstrap_local_legacy_credentials(pbgdir)
    return coordinator.run()


def local_legacy_credential_inventory(
    pbgdir: Path | str,
    *,
    require_barrier: bool = False,
) -> tuple[dict[str, str], dict[str, int]]:
    """Compute this node's own value-free legacy source inventory."""

    coordinator = CredentialMigrationCoordinator(pbgdir)
    if require_barrier:
        materialized = rebuild_materialized_state(coordinator.cluster_root, write=False)
        migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
        active = coordinator._active_nodes(materialized)
        freeze_acks = migration.get("freeze_acks") if isinstance(migration.get("freeze_acks"), dict) else {}
        generation = int(migration.get("freeze_generation") or 0)
        if migration.get("frozen") is not True or any(
            not coordinator._freeze_ack_matches(freeze_acks.get(node_id), generation)
            for node_id in active
        ):
            raise CredentialMigrationBlocked("Legacy credential inventory requires the frozen writer barrier")
    sources = coordinator._inventory_sources()
    return (
        {source.source_key: source.fingerprint for source in sources.values()},
        {source.source_key: source.generation for source in sources.values()},
    )


def local_managed_credential_scan(pbgdir: Path | str) -> list[str]:
    """Scan local legacy values across every PBGui-managed post-freeze location."""

    coordinator = CredentialMigrationCoordinator(pbgdir)
    materialized = rebuild_materialized_state(coordinator.cluster_root, write=False)
    migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
    if migration.get("frozen") is not True:
        raise CredentialMigrationBlocked("Managed credential scan requires the frozen writer barrier")
    findings = coordinator._scan_for_legacy_values({})
    return sorted(set(str(item)[:240] for item in findings))[:256]


def advance_local_credential_migration(
    pbgdir: Path | str,
    *,
    max_items: int = 8,
    scan_allowed: bool = True,
) -> dict[str, Any]:
    """Advance one bounded local freeze/candidate/import/cleanup turn."""

    root = _lexical_absolute(pbgdir)
    from credential_rolling_bootstrap import bootstrap_local_legacy_credentials

    bootstrap_local_legacy_credentials(root)
    cluster_root = default_cluster_root(root)
    materialized = rebuild_materialized_state(cluster_root, write=False)
    desired = materialized.get("desired_state") or {}
    migration = desired.get("credential_migration") or {}
    if migration.get("frozen") is not True:
        return {"status": "idle"}
    barrier_ack = _append_credential_migration_acks(cluster_root, scan_allowed=False)
    materialized = rebuild_materialized_state(cluster_root, write=False)
    desired = materialized.get("desired_state") or {}
    migration = desired.get("credential_migration") or {}
    active_nodes = CredentialMigrationCoordinator._active_nodes(materialized)
    freeze_acks = migration.get("freeze_acks") if isinstance(migration.get("freeze_acks"), dict) else {}
    if not set(active_nodes).issubset(freeze_acks):
        return {
            "status": "waiting_for_freeze_acks",
            "ack": barrier_ack,
            "missing_node_ids": sorted(set(active_nodes) - set(freeze_acks)),
        }
    identity = read_local_identity(cluster_root)
    node_id = str(identity["node_id"])
    node = (((materialized.get("cluster_nodes") or {}).get("nodes") or {}).get(node_id) or {})
    role = str(node.get("role") or "")
    coordinator = CredentialMigrationCoordinator(root)
    sources = coordinator._inventory_sources()
    result: dict[str, Any] = {
        "status": "advanced",
        "role": role,
        "inventoried_sources": len(sources),
        "published_candidates": 0,
        "accepted_candidates": 0,
        "imported_local": 0,
        "cleaned_sources": 0,
    }
    if role == "vps":
        result["published_candidates"] = _publish_local_migration_candidates(
            coordinator,
            materialized,
            sources,
            max_items=max_items,
        )
    elif role == "master":
        result["accepted_candidates"] = _accept_remote_migration_candidates(
            coordinator,
            materialized,
            max_items=max_items,
        )
        if not _coordinator_owns_current_freeze(coordinator, migration):
            result["imported_local"] = _import_local_master_sources(
                coordinator,
                sources,
                migration,
                max_items=max_items,
            )
    materialized = rebuild_materialized_state(cluster_root, write=False)
    result["cleaned_sources"] = _cleanup_accepted_local_sources(
        coordinator,
        materialized,
        sources,
        role=role,
    )
    result["ack"] = _append_credential_migration_acks(
        cluster_root,
        scan_allowed=scan_allowed,
    )
    return result


def _coordinator_owns_current_freeze(
    coordinator: CredentialMigrationCoordinator,
    migration: Mapping[str, Any],
) -> bool:
    """Return whether this node's coordinator initiated the replicated freeze."""

    try:
        state = coordinator._read_state()
    except Exception:
        return False
    return (
        str(state.get("operation_id") or "")
        == str(migration.get("migration_operation_id") or "")
        and int(state.get("freeze_generation") or 0)
        == int(migration.get("freeze_generation") or 0)
    )


def _migration_candidate_id(
    node_id: str,
    freeze_generation: int,
    source: _Source,
    item_id: str,
) -> str:
    """Derive a stable opaque identifier for one local source item generation."""

    digest = hashlib.sha256(
        f"{node_id}\0{freeze_generation}\0{source.source_key}\0{source.fingerprint}\0{item_id}".encode(
            "utf-8"
        )
    ).hexdigest()
    return f"candidate_{digest[:40]}"


def _publish_local_migration_candidates(
    coordinator: CredentialMigrationCoordinator,
    materialized: dict[str, Any],
    sources: Mapping[str, _Source],
    *,
    max_items: int,
) -> int:
    """Seal each VPS legacy value only to active masters and append metadata."""

    desired = materialized.get("desired_state") or {}
    migration = desired.get("credential_migration") or {}
    freeze_generation = int(migration.get("freeze_generation") or 0)
    existing = migration.get("candidates") if isinstance(migration.get("candidates"), dict) else {}
    identity = read_local_identity(coordinator.cluster_root)
    node_id = str(identity["node_id"])
    publisher = ClusterCredentialPublisher(coordinator.cluster_root, coordinator.store)
    keys = ensure_node_key_material(coordinator.cluster_root)
    recipients, _roles = publisher._audience_recipients(materialized, "masters")
    recipient_ids = [recipient.node_id for recipient in recipients]
    recipient_key_ids = publisher._recipient_key_ids(materialized, recipient_ids)
    membership_generation = int(
        (materialized.get("cluster_nodes") or {}).get("credential_membership_generation") or 0
    )
    created = 0
    for source in sorted(sources.values(), key=lambda item: item.source_id):
        for item in source.items:
            if created >= max_items:
                return created
            candidate_id = _migration_candidate_id(
                node_id,
                freeze_generation,
                source,
                str(item["item_id"]),
            )
            current = existing.get(candidate_id) if isinstance(existing.get(candidate_id), dict) else {}
            if (
                current
                and int(current.get("membership_generation") or -1) == membership_generation
                and list(current.get("recipient_ids") or []) == recipient_ids
                and dict(current.get("recipient_key_ids") or {}) == recipient_key_ids
            ):
                continue
            kind = str(item["kind"])
            context = SecretContext(
                cluster_id=str(desired["cluster_id"]),
                secret_id=candidate_id,
                kind=f"migration_{kind}",
                generation=freeze_generation,
                audience="masters",
            )
            plaintext: dict[str, Any] = {"kind": kind, "value": item["value"]}
            from credential_rolling_bootstrap import legacy_shadow_credential_id

            shadow_id = legacy_shadow_credential_id(
                coordinator.root,
                source.source_id,
                source.fingerprint,
                str(item["item_id"]),
            )
            if shadow_id:
                plaintext["legacy_shadow_credential_id"] = shadow_id
            if kind == "tradfi_profile":
                plaintext["provider"] = str(item.get("provider") or "unknown")
            envelope = seal_secret(
                canonical_json_bytes(plaintext),
                context,
                recipients,
                keys.signing_private_key,
                signer_id=node_id,
            )
            raw = serialize_sealed_secret(envelope)
            blob_hash, blob_path, blob_created = publisher._write_sealed_blob(raw)
            try:
                append_operation(
                    coordinator.cluster_root,
                    "MIGRATION_SECRET_CANDIDATE",
                    {
                        "candidate_id": candidate_id,
                        "candidate_kind": kind,
                        "freeze_generation": freeze_generation,
                        "source_fingerprint": source.fingerprint,
                        "source_generation": source.generation,
                        "sealed_blob_hash": blob_hash,
                        "audience": "masters",
                        "membership_generation": membership_generation,
                        "recipient_ids": recipient_ids,
                        "recipient_key_ids": recipient_key_ids,
                        "node_id": node_id,
                    },
                )
            except Exception:
                if blob_created:
                    blob_path.unlink(missing_ok=True)
                raise
            created += 1
    return created


def _accept_remote_migration_candidates(
    coordinator: CredentialMigrationCoordinator,
    materialized: dict[str, Any],
    *,
    max_items: int,
) -> int:
    """Import VPS candidates on the deterministic authority master and sign acceptance."""

    nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
    masters = sorted(
        str(node_id)
        for node_id, node in nodes.items()
        if isinstance(node, dict)
        and node.get("enabled", True) is not False
        and node.get("state_replica", True) is not False
        and str(node.get("role") or "") == "master"
    )
    local_node_id = str(read_local_identity(coordinator.cluster_root)["node_id"])
    if not masters or masters[0] != local_node_id:
        return 0
    migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
    candidates = migration.get("candidates") if isinstance(migration.get("candidates"), dict) else {}
    acceptances = migration.get("candidate_acceptances") if isinstance(migration.get("candidate_acceptances"), dict) else {}
    imported: list[tuple[str, str, int]] = []
    for candidate_id, candidate in sorted(candidates.items()):
        if len(imported) >= max_items or candidate_id in acceptances or not isinstance(candidate, dict):
            continue
        credential_id, generation = _materialize_candidate(coordinator, materialized, candidate)
        if not credential_id:
            continue
        imported.append((candidate_id, credential_id, generation))
    if imported:
        from credential_reconciler import reconcile_pending_credentials

        reconcile_pending_credentials(
            coordinator.root,
            store=coordinator.store,
            publisher=ClusterCredentialPublisher(coordinator.cluster_root, coordinator.store),
        )
    accepted = 0
    for candidate_id, credential_id, generation in imported:
        kind = "cmc" if credential_id.startswith("cmc_") else "tradfi"
        record = (
            coordinator.store.get_cmc(credential_id)
            if kind == "cmc"
            else coordinator.store.get_tradfi(credential_id)
        )
        if record.get("pending"):
            continue
        append_operation(
            coordinator.cluster_root,
            "MIGRATION_SECRET_ACCEPTANCE",
            {
                "candidate_id": candidate_id,
                "credential_id": credential_id,
                "credential_generation": generation,
                "freeze_generation": int(migration.get("freeze_generation") or 0),
                "status": "accepted",
            },
        )
        accepted += 1
    return accepted


def _materialize_candidate(
    coordinator: CredentialMigrationCoordinator,
    materialized: dict[str, Any],
    candidate: Mapping[str, Any],
) -> tuple[str, int]:
    """Authenticate, decrypt, and stage one candidate without exposing its value."""

    from cluster_sync_command import _open_with_local_key_history

    candidate_id = str(candidate["candidate_id"])
    kind = str(candidate["candidate_kind"])
    freeze_generation = int(candidate["freeze_generation"])
    context = SecretContext(
        cluster_id=str((materialized.get("desired_state") or {}).get("cluster_id") or ""),
        secret_id=candidate_id,
        kind=f"migration_{kind}",
        generation=freeze_generation,
        audience="masters",
    )
    raw = _read_content_blob(
        ClusterPaths.from_root(coordinator.cluster_root).sealed_blobs,
        str(candidate["sealed_blob_hash"]),
    )
    envelope = deserialize_sealed_secret(raw)
    if str(envelope.get("signer_id") or "") != str(candidate.get("submitted_by") or ""):
        raise CredentialMigrationError("Migration candidate signer mismatch")
    envelope_ids = [str(item.get("node_id") or "") for item in envelope.get("recipients") or []]
    if envelope_ids != list(candidate.get("recipient_ids") or []):
        raise CredentialMigrationError("Migration candidate recipients mismatch")
    public_key = membership_signing_public_key(
        coordinator.cluster_root,
        str(envelope["signer_id"]),
        str(envelope["signing_key_id"]),
    )
    plaintext = _open_with_local_key_history(
        coordinator.cluster_root,
        envelope,
        str(read_local_identity(coordinator.cluster_root)["node_id"]),
        public_key,
        context,
        {str(node_id): "master" for node_id in candidate.get("recipient_ids") or []},
    )
    try:
        payload = json.loads(plaintext.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CredentialMigrationError("Migration candidate plaintext is invalid") from exc
    if not isinstance(payload, dict) or payload.get("kind") != kind:
        raise CredentialMigrationError("Migration candidate kind mismatch")
    credential_id, generation = _resolve_migration_import(
        coordinator,
        materialized,
        candidate_id=candidate_id,
        kind=kind,
        value=payload.get("value"),
        provider=str(payload.get("provider") or ""),
        label="Imported remote legacy credential",
        freeze_generation=freeze_generation,
        preferred_credential_id=str(payload.get("legacy_shadow_credential_id") or ""),
    )
    if credential_id:
        _stage_migration_import(coordinator, candidate_id, credential_id)
    return credential_id, generation


def _resolve_migration_import(
    coordinator: CredentialMigrationCoordinator,
    materialized: dict[str, Any],
    *,
    candidate_id: str,
    kind: str,
    value: Any,
    provider: str,
    label: str,
    freeze_generation: int,
    preferred_credential_id: str = "",
) -> tuple[str, int]:
    """Constant-time dedupe or signed conflict resolution shared by every origin."""

    if kind not in {"cmc_api_key", "tradfi_profile"}:
        raise CredentialMigrationError("Unsupported migration credential kind")
    provider = str(provider or "unknown").strip().lower() if kind == "tradfi_profile" else ""
    latest = rebuild_materialized_state(coordinator.cluster_root, write=False)
    migration = (latest.get("desired_state") or {}).get("credential_migration") or {}
    conflicts = migration.get("conflicts") if isinstance(migration.get("conflicts"), dict) else {}
    candidate_conflicts = sorted(
        (
            conflict for conflict in conflicts.values()
            if isinstance(conflict, dict) and str(conflict.get("candidate_id") or "") == candidate_id
        ),
        key=lambda conflict: str(conflict.get("conflict_id") or ""),
    )
    for conflict in candidate_conflicts:
        resolution = conflict.get("resolution") if isinstance(conflict.get("resolution"), dict) else {}
        choice = str(resolution.get("choice") or "")
        if not choice:
            _block_tradfi_migration_activation(coordinator, provider)
            return "", 0
        if choice == "existing":
            credential_id = str(conflict["existing_credential_id"])
            record = coordinator.store.get_tradfi(credential_id)
            return credential_id, int(record["generation"])

    if kind == "cmc_api_key":
        records = coordinator.store.list_cmc(active_only=False)
        for record in records:
            try:
                existing = coordinator.store.load_cmc_key(
                    str(record["id"]), int(record["generation"])
                )
            except (KeyError, ValueError):
                continue
            if _migration_values_equal(existing, value):
                return str(record["id"]), int(record["generation"])
    else:
        records = [
            record for record in coordinator.store.list_tradfi(active_only=False)
            if str(record.get("provider") or "").strip().lower() == provider
        ]
        for record in records:
            try:
                existing = coordinator.store.load_tradfi_credentials(
                    str(record["id"]), int(record["generation"])
                )
            except (KeyError, ValueError):
                continue
            if _migration_values_equal(existing, value):
                return str(record["id"]), int(record["generation"])
        if records and not candidate_conflicts:
            existing = sorted(records, key=lambda record: str(record["id"]))[0]
            conflict_id = "migration_conflict_" + hashlib.sha256(
                f"{candidate_id}\0{existing['id']}\0{provider}".encode("utf-8")
            ).hexdigest()[:32]
            append_operation(
                coordinator.cluster_root,
                "MIGRATION_SECRET_CONFLICT",
                {
                    "conflict_id": conflict_id,
                    "candidate_id": candidate_id,
                    "existing_credential_id": str(existing["id"]),
                    "provider": provider,
                    "freeze_generation": freeze_generation,
                    "status": "unresolved",
                },
            )
            _block_tradfi_migration_activation(coordinator, provider)
            return "", 0

    prefix = "cmc" if kind == "cmc_api_key" else "tradfi"
    credential_id = str(preferred_credential_id or "")
    if not credential_id.startswith(f"{prefix}_"):
        credential_id = f"{prefix}_{hashlib.sha256(candidate_id.encode('utf-8')).hexdigest()[:32]}"
    metadata: dict[str, Any] = {"label": label, "active": False}
    if kind == "tradfi_profile":
        metadata["provider"] = provider
    record = coordinator.store.materialize_cluster_secret(
        credential_id,
        kind,
        1,
        value,
        metadata=metadata,
    )
    return credential_id, int(record["generation"])


def _block_tradfi_migration_activation(
    coordinator: CredentialMigrationCoordinator,
    provider: str,
) -> None:
    """Keep a conflicted provider inactive locally and in signed Cluster state."""

    for record in coordinator.store.list_tradfi(active_only=False):
        if str(record.get("provider") or "").strip().lower() != provider:
            continue
        if record.get("active"):
            coordinator.store.update_tradfi(str(record["id"]), active=False)
    ClusterCredentialPublisher(
        coordinator.cluster_root,
        coordinator.store,
    ).set_tradfi_active_profile(provider, None)


def _stage_migration_import(
    coordinator: CredentialMigrationCoordinator,
    candidate_id: str,
    credential_id: str,
) -> None:
    """Put one resolved forced-command import through the durable pending lifecycle."""

    operation_id = f"migration:{candidate_id}"
    if credential_id.startswith("cmc_"):
        record = coordinator.store.get_cmc(credential_id)
        if operation_id not in {
            record.get("pending_operation_id"),
            record.get("last_operation_id"),
        }:
            coordinator.store.update_cmc(
                credential_id,
                active=True,
                pending=True,
                operation_id=operation_id,
            )
    else:
        record = coordinator.store.get_tradfi(credential_id)
        if operation_id not in {
            record.get("pending_operation_id"),
            record.get("last_operation_id"),
        }:
            coordinator.store.update_tradfi(
                credential_id,
                active=True,
                pending=True,
                operation_id=operation_id,
            )


def _unresolved_migration_conflicts(materialized: Mapping[str, Any]) -> list[str]:
    """Return secret-free IDs for all unresolved migration conflicts."""

    migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
    conflicts = migration.get("conflicts") if isinstance(migration.get("conflicts"), dict) else {}
    return sorted(
        str(conflict_id)
        for conflict_id, conflict in conflicts.items()
        if isinstance(conflict, dict) and str(conflict.get("status") or "") != "resolved"
    )


def resolve_migration_conflict(
    pbgdir: Path | str,
    conflict_id: str,
    choice: str,
    *,
    resolution_id: str,
) -> dict[str, Any]:
    """Persist an authenticated, secret-free administrator conflict decision."""

    selected = str(choice).strip().lower()
    if selected not in {"candidate", "existing"}:
        raise CredentialMigrationError("Migration conflict choice must be candidate or existing")
    if not str(resolution_id).strip():
        raise CredentialMigrationError("Migration conflict resolution_id is required")
    coordinator = CredentialMigrationCoordinator(pbgdir)
    materialized = rebuild_materialized_state(coordinator.cluster_root, write=False)
    migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
    conflicts = migration.get("conflicts") if isinstance(migration.get("conflicts"), dict) else {}
    conflict = conflicts.get(str(conflict_id)) if isinstance(conflicts.get(str(conflict_id)), dict) else None
    if conflict is None:
        raise CredentialMigrationError("Migration conflict was not found")
    current = conflict.get("resolution") if isinstance(conflict.get("resolution"), dict) else None
    if current is not None:
        if str(current.get("choice") or "") != selected or str(current.get("resolution_id") or "") != str(resolution_id):
            raise CredentialMigrationError("Migration conflict already has another resolution")
        return dict(conflict)
    operation = append_operation(
        coordinator.cluster_root,
        "MIGRATION_SECRET_CONFLICT_RESOLUTION",
        {
            "conflict_id": str(conflict_id),
            "choice": selected,
            "resolution_id": str(resolution_id),
        },
    )
    result = dict(conflict)
    result.update({
        "status": "resolved",
        "resolution": {
            "choice": selected,
            "resolution_id": str(resolution_id),
            "op_id": str(operation["op_id"]),
        },
    })
    return result


def _import_local_master_sources(
    coordinator: CredentialMigrationCoordinator,
    sources: Mapping[str, _Source],
    migration: Mapping[str, Any],
    *,
    max_items: int,
) -> int:
    """Stage a remote master's own legacy values through normal pending lifecycle."""

    node_id = str(read_local_identity(coordinator.cluster_root)["node_id"])
    freeze_generation = int(migration.get("freeze_generation") or 0)
    acceptances = migration.get("candidate_acceptances") if isinstance(migration.get("candidate_acceptances"), dict) else {}
    staged: list[tuple[str, str, int]] = []
    for source in sorted(sources.values(), key=lambda item: item.source_id):
        for item in source.items:
            if len(staged) >= max_items:
                break
            candidate_id = _migration_candidate_id(node_id, freeze_generation, source, str(item["item_id"]))
            if candidate_id in acceptances:
                continue
            kind = str(item["kind"])
            credential_id, generation = _resolve_migration_import(
                coordinator,
                rebuild_materialized_state(coordinator.cluster_root, write=False),
                candidate_id=candidate_id,
                kind=kind,
                value=item["value"],
                provider=str(item.get("provider") or ""),
                label=str(item.get("label") or "Imported local legacy credential"),
                freeze_generation=freeze_generation,
            )
            if not credential_id:
                continue
            _stage_migration_import(coordinator, candidate_id, credential_id)
            staged.append((candidate_id, credential_id, generation))
    if staged:
        from credential_reconciler import reconcile_pending_credentials

        reconcile_pending_credentials(
            coordinator.root,
            store=coordinator.store,
            publisher=ClusterCredentialPublisher(coordinator.cluster_root, coordinator.store),
        )
    accepted = 0
    for candidate_id, credential_id, generation in staged:
        record = (
            coordinator.store.get_cmc(credential_id)
            if credential_id.startswith("cmc_")
            else coordinator.store.get_tradfi(credential_id)
        )
        if record.get("pending"):
            continue
        append_operation(
            coordinator.cluster_root,
            "MIGRATION_SECRET_ACCEPTANCE",
            {
                "candidate_id": candidate_id,
                "credential_id": credential_id,
                "credential_generation": generation,
                "freeze_generation": freeze_generation,
                "status": "accepted",
            },
        )
        accepted += 1
    return accepted


def _cleanup_accepted_local_sources(
    coordinator: CredentialMigrationCoordinator,
    materialized: dict[str, Any],
    sources: Mapping[str, _Source],
    *,
    role: str,
) -> int:
    """Remove unchanged local fields only after acceptance, cutoff, and cleanup ACK."""

    migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
    if _unresolved_migration_conflicts(materialized):
        return 0
    cutoff = migration.get("cutoff") if isinstance(migration.get("cutoff"), dict) else None
    if cutoff is None:
        return 0
    node_id = str(read_local_identity(coordinator.cluster_root)["node_id"])
    cleanup_acks = migration.get("cleanup_acks") if isinstance(migration.get("cleanup_acks"), dict) else {}
    cleanup_ack = cleanup_acks.get(node_id) if isinstance(cleanup_acks.get(node_id), dict) else {}
    if int(cleanup_ack.get("cutoff_generation") or 0) != int(cutoff.get("cutoff_generation") or 0):
        return 0
    acceptances = migration.get("candidate_acceptances") if isinstance(migration.get("candidate_acceptances"), dict) else {}
    freeze_generation = int(migration.get("freeze_generation") or 0)
    cleaned = 0
    for source in sorted(sources.values(), key=lambda item: item.source_id):
        item_ids = [
            _migration_candidate_id(node_id, freeze_generation, source, str(item["item_id"]))
            for item in source.items
        ]
        if any(candidate_id not in acceptances for candidate_id in item_ids):
            continue
        if role == "master":
            incomplete = False
            for candidate_id in item_ids:
                acceptance = acceptances.get(candidate_id) if isinstance(acceptances.get(candidate_id), dict) else {}
                credential_id = str(acceptance.get("credential_id") or "")
                try:
                    record = (
                        coordinator.store.get_cmc(credential_id)
                        if credential_id.startswith("cmc_")
                        else coordinator.store.get_tradfi(credential_id)
                    )
                except KeyError:
                    incomplete = True
                    break
                if record.get("pending"):
                    incomplete = True
                    break
            if incomplete:
                continue
        with coordinator._legacy_source_lock(source.kind, source.path):
            if _file_fingerprint(source.path) != source.fingerprint:
                continue
            backup_root = coordinator.backup_root / "remote-local"
            ensure_private_directory_tree(coordinator.migration_root, backup_root)
            backup = backup_root / f"{hashlib.sha256(source.source_id.encode()).hexdigest()[:24]}.bak"
            if not backup.exists():
                atomic_write_private_bytes(backup, source.path.read_bytes())
            if source.kind == "pb7":
                PB7ApiKeysMergeWriter(
                    source.path,
                    coordinator.store.root / "pb7_projection.json",
                ).project_tradfi(None, source_fingerprint=f"migration-cleanup:{source.fingerprint}")
            else:
                atomic_write_private_bytes(source.path, coordinator._cleaned_source_bytes(source))
            cleaned += 1
    return cleaned


def credential_migration_restart_block_reason(pbgdir: Path | str | None = None) -> str:
    """Return the persisted unsafe-phase reason used by API restart gating."""

    root = Path(pbgdir or Path(__file__).resolve().parent)
    state_path = root / "data" / "credentials" / "migration" / "state.json"
    if state_path.is_symlink():
        return "Credential migration state is unsafe"
    if not state_path.is_file():
        return ""
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "Credential migration state is unreadable"
    if not isinstance(state, dict):
        return ""
    if state.get("status") == "waiting_for_upgrade":
        return ""
    blocker_reason = str(state.get("blocker_reason") or "")
    passive_wait_prefixes = (
        "Waiting for writer-freeze ACK from active Cluster nodes:",
        "Waiting for credential inventory ACK from active Cluster nodes:",
        "Waiting for credential materialization ACK from active Cluster nodes:",
        "Waiting for credential cutoff cleanup ACK from active Cluster nodes:",
        "Waiting for credential scan ACK from active Cluster nodes:",
    )
    if blocker_reason.startswith(passive_wait_prefixes):
        return ""
    if state.get("phase") == "complete" and not state.get("blocker_reason"):
        return ""
    return blocker_reason or f"Credential migration phase {state.get('phase') or 'unknown'} is active"


def persist_credential_migration_error(
    reason: str,
    pbgdir: Path | str | None = None,
) -> None:
    """Best-effort persistence for coordinator construction/startup failures."""

    root = Path(pbgdir or Path(__file__).resolve().parent)
    migration_root = root / "data" / "credentials" / "migration"
    state_path = migration_root / "state.json"
    ensure_private_directory(migration_root)
    with advisory_file_lock(state_path):
        state: dict[str, Any] = {
            "version": STATE_VERSION,
            "phase": "protocol_barrier",
            "operation_id": uuid.uuid4().hex,
            "sources": {},
            "source_results": {},
            "operation_ids": [],
            "started_at": _timestamp(),
        }
        if state_path.is_file():
            try:
                loaded = json.loads(state_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict) and loaded.get("version") == STATE_VERSION:
                    state = loaded
            except (OSError, json.JSONDecodeError):
                pass
        state["blocker_reason"] = str(reason or "Credential migration failed")
        state["updated_at"] = _timestamp()
        atomic_write_private_text(state_path, json.dumps(state, indent=4, sort_keys=True) + "\n")


def _safe_migration_error(exc: Exception) -> str:
    """Return a blocker reason that cannot echo malformed INI source lines."""

    if isinstance(exc, configparser.Error) or isinstance(exc.__cause__, configparser.Error):
        return "CredentialMigrationError: Unable to parse legacy INI source"
    return f"{type(exc).__name__}: {exc}"


def _lexical_absolute(path: Path | str) -> Path:
    """Return an absolute lexical path without resolving symlink components."""

    return Path(os.path.abspath(Path(path).expanduser()))


def _reject_symlink_components(path: Path) -> None:
    """Reject every existing symlink component using lstat."""

    candidate = _lexical_absolute(path)
    current = Path(candidate.anchor)
    for part in candidate.parts[1:]:
        current = current / part
        try:
            mode = os.lstat(current).st_mode
        except FileNotFoundError:
            break
        if stat.S_ISLNK(mode):
            raise CredentialMigrationError(f"Refusing symlinked migration path: {current}")


def _content_blob_path(root: Path, blob_hash: str) -> Path:
    """Return a validated content-addressed Cluster blob path."""

    value = str(blob_hash)
    digest = value.removeprefix("sha256:")
    if not value.startswith("sha256:") or len(digest) != 64:
        raise CredentialMigrationError("Legacy API-key blob reference is invalid")
    try:
        int(digest, 16)
    except ValueError as exc:
        raise CredentialMigrationError("Legacy API-key blob reference is invalid") from exc
    return root / "sha256" / digest[:2] / f"{digest}.json"


def _read_content_blob(root: Path, blob_hash: str) -> bytes:
    """Read and hash-verify one local Cluster content blob."""

    path = _content_blob_path(root, blob_hash)
    _reject_symlink_components(path)
    if not path.is_file():
        raise CredentialMigrationBlocked("Legacy API-key checkpoint blob is not available locally")
    secure_private_file(path)
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != str(blob_hash).removeprefix("sha256:"):
        raise CredentialMigrationError("Legacy API-key checkpoint blob hash mismatch")
    return raw


def _write_content_blob(root: Path, raw: bytes) -> str:
    """Write one owner-only content-addressed Cluster blob."""

    digest = hashlib.sha256(raw).hexdigest()
    blob_hash = f"sha256:{digest}"
    path = _content_blob_path(root, blob_hash)
    ensure_private_directory_tree(root, path.parent)
    _reject_symlink_components(path)
    if path.exists():
        secure_private_file(path)
        if path.read_bytes() != raw:
            raise CredentialMigrationError("Cluster content blob hash collision")
        return blob_hash
    atomic_write_private_bytes(path, raw)
    return blob_hash


def _redact_legacy_api_keys(value: Any) -> Any:
    """Build non-secret hash metadata for the exchange-only checkpoint."""

    secret_fields = {
        "key", "secret", "passphrase", "private_key", "wallet_address",
        "api_key", "api_secret", "token", "access_token",
    }
    if isinstance(value, dict):
        return {
            str(key): (
                "<redacted>" if str(key).lower() in secret_fields and item is not None and item != ""
                else _redact_legacy_api_keys(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_legacy_api_keys(item) for item in value]
    return value


def _file_fingerprint(path: Path) -> str:
    """Return the exact SHA-256 generation fingerprint for one source file."""

    if path.is_symlink() or not path.is_file():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _usable_secret(value: Any) -> bool:
    """Reject empty and documented placeholder values without transforming secrets."""

    if not isinstance(value, str) or not value.strip():
        return False
    stripped = value.strip()
    if stripped.lower() in {"none", "null", "false", "<api_key>"}:
        return False
    return not (stripped.startswith("<") and stripped.endswith(">"))


def _json_field_values(value: Any, field: str, location: str = "$") -> list[tuple[str, Any]]:
    """Return all exact legacy JSON fields without rendering their values."""

    result: list[tuple[str, Any]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == field:
                result.append((location, item))
            result.extend(_json_field_values(item, field, f"{location}.{key}"))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            result.extend(_json_field_values(item, field, f"{location}[{index}]"))
    return result


def _remove_json_field(value: Any, field: str) -> None:
    """Remove one exact field recursively from a JSON-compatible value."""

    if isinstance(value, dict):
        value.pop(field, None)
        for item in value.values():
            _remove_json_field(item, field)
    elif isinstance(value, list):
        for item in value:
            _remove_json_field(item, field)


def _json_secret_locations(
    value: Any,
    sentinels: set[str],
    location: str = "$",
) -> list[str]:
    """Return redacted JSON field paths containing migrated values or retired names."""

    findings: list[str] = []
    retired = {LEGACY_VPS_FIELD, *CMC_INI_FIELDS, *TRADFI_INI_FIELDS}
    if isinstance(value, dict):
        for key, item in value.items():
            field = f"{location}.{key}"
            if str(key) in retired or (
                isinstance(item, str) and any(sentinel in item for sentinel in sentinels)
            ):
                findings.append(field)
            findings.extend(_json_secret_locations(item, sentinels, field))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            findings.extend(_json_secret_locations(item, sentinels, f"{location}[{index}]"))
    return findings


def _argv_path_is_below(argument: Any, root: Path) -> bool:
    """Return whether one argv element is an existing path below the PBGui root."""

    text = str(argument or "").strip()
    if not text or "\x00" in text:
        return False
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        return False
    candidate = _lexical_absolute(candidate)
    approved = _lexical_absolute(root)
    return candidate == approved or approved in candidate.parents


def _path_is_below(path: Path, root: Path) -> bool:
    """Compare lexical paths without following a potentially hostile symlink."""

    candidate = _lexical_absolute(path)
    approved = _lexical_absolute(root)
    return candidate == approved or approved in candidate.parents


def _legacy_backup_values(raw: bytes) -> set[str]:
    """Recover credential values from a private legacy backup without retaining metadata."""

    values: set[str] = set()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return values

    parser = configparser.ConfigParser()
    try:
        parser.read_string(text)
    except configparser.Error:
        parser = configparser.ConfigParser()
    if parser.has_section("coinmarketcap"):
        value = parser.get("coinmarketcap", "api_key", fallback="")
        if _usable_secret(value):
            values.add(value)
    if parser.has_section("tradfi_profiles"):
        for field in TRADFI_INI_FIELDS:
            value = parser.get("tradfi_profiles", field, fallback="")
            if _usable_secret(value):
                values.add(value)

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return values
    values.update(
        str(value)
        for _location, value in _json_field_values(payload, LEGACY_VPS_FIELD)
        if _usable_secret(value)
    )
    if not isinstance(payload, dict) or not isinstance(payload.get("tradfi"), dict):
        return values
    tradfi = payload["tradfi"]
    profiles = tradfi.get("profiles") if isinstance(tradfi.get("profiles"), dict) else {}
    for profile in profiles.values():
        if isinstance(profile, Mapping):
            values.update(_tradfi_secret_mapping(profile).values())
    for provider in TRADFI_PROVIDERS:
        profile = tradfi.get(provider)
        if isinstance(profile, Mapping):
            values.update(_tradfi_secret_mapping(profile).values())
    values.update(_tradfi_secret_mapping(tradfi).values())
    return values


def _file_contains_any(path: Path, needles: list[bytes]) -> bool:
    """Search a managed file in bounded chunks without rendering its content."""

    if not needles:
        return False
    overlap = max((len(needle) for needle in needles), default=1) - 1
    previous = b""
    try:
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    return False
                window = previous + chunk
                if any(needle in window for needle in needles):
                    return True
                previous = window[-overlap:] if overlap > 0 else b""
    except OSError:
        return False


def _tradfi_secret_mapping(profile: Mapping[str, Any]) -> dict[str, str]:
    """Extract scalar legacy provider credentials while excluding projection metadata."""

    metadata = {
        "provider",
        "label",
        "generation",
        "active",
        "active_profile_id",
        "profiles",
        "enabled",
        "name",
        "_projection_generation",
        "_source_fingerprint",
    }
    return {
        str(key): value
        for key, value in profile.items()
        if key not in metadata and isinstance(value, str) and _usable_secret(value)
    }


def _migration_values_equal(left: Any, right: Any) -> bool:
    """Compare transient migration plaintexts without persisting a derived digest."""

    try:
        left_bytes = canonical_json_bytes(left)
        right_bytes = canonical_json_bytes(right)
    except Exception:
        return False
    return hmac.compare_digest(left_bytes, right_bytes)


def _timestamp() -> str:
    """Return an explicit UTC timestamp for persisted progress."""

    return datetime.now(timezone.utc).isoformat()


__all__ = [
    "CMC_INI_FIELDS",
    "CredentialMigrationBlocked",
    "CredentialMigrationCoordinator",
    "CredentialMigrationError",
    "PHASES",
    "TRADFI_INI_FIELDS",
    "credential_migration_restart_block_reason",
    "persist_credential_migration_error",
    "resolve_migration_conflict",
    "local_legacy_credential_inventory",
    "local_managed_credential_scan",
    "run_credential_migration",
]
