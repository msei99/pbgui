"""Publish local credentials into Cluster Sync v2 sealed state."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Callable, Mapping

from cluster_credentials import (
    complete_node_key_rotation,
    SecretContext,
    SecretRecipient,
    canonical_json_bytes,
    deserialize_sealed_secret,
    ensure_node_key_material,
    seal_secret,
    serialize_sealed_secret,
    validate_sealed_secret,
)
from credential_store import CredentialStore
from file_lock import advisory_file_lock
from master.cluster_state import (
    CMC_KEY_STATES,
    CRYPTO_PUBLIC_FIELDS,
    ClusterPaths,
    append_operation,
    load_operations,
    membership_signing_public_key,
    read_local_identity,
    rebuild_materialized_state,
    rotate_local_node_keys,
)
from secure_files import (
    atomic_write_private_bytes,
    ensure_private_directory_tree,
    secure_private_file,
)
from operation_store import DurableOperationStore


SERVICE = "ClusterCredentialPublisher"

_SECRET_KINDS = frozenset({"cmc_api_key", "tradfi_profile"})
_CMC_POOL_FIELDS = frozenset({
    "quota_domain_id",
    "provider_plan",
    "minute_limit",
    "daily_limit",
    "monthly_limit",
})


class CredentialPublicationError(RuntimeError):
    """Raised when a credential cannot be safely published."""


class ClusterCredentialPublisher:
    """Seal credentials and append their signed Cluster Sync v2 metadata."""

    def __init__(self, cluster_root: Path | str, credential_store: CredentialStore) -> None:
        """Bind the publisher to one cluster and one local credential store."""

        if not isinstance(credential_store, CredentialStore):
            raise TypeError("credential_store must be a CredentialStore")
        self.cluster_root = Path(cluster_root).expanduser().resolve(strict=False)
        self.credential_store = credential_store
        self.paths = ClusterPaths.from_root(self.cluster_root)

    def publish_cmc(
        self,
        credential_id: str,
        generation: int | None = None,
        *,
        state: str | None = None,
        pool_metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Publish one existing CMC generation to all active state replicas."""

        record = self.credential_store.get_cmc(credential_id)
        selected_generation = self._selected_generation(record, generation)
        target_state = str(state or ("active" if record.get("active", True) else "disabled"))
        if target_state not in CMC_KEY_STATES:
            raise CredentialPublicationError(f"unsupported CMC key state: {target_state}")
        extra_pool = dict(pool_metadata or {})
        unknown_pool = sorted(set(extra_pool) - _CMC_POOL_FIELDS)
        if unknown_pool:
            raise CredentialPublicationError(
                f"unsupported CMC pool metadata: {', '.join(unknown_pool)}"
            )

        with advisory_file_lock(self.paths.root / ".append_sequence"):
            keys, materialized = self._ensure_local_crypto_membership()
            recipients, membership_roles = self._audience_recipients(materialized, "cluster")
            operation_ids: list[str] = []
            cas: dict[str, int] = {}
            blob_hash, secret_operation = self._ensure_secret_publication(
                materialized,
                keys,
                recipients,
                membership_roles,
                record,
                selected_generation,
                kind="cmc_api_key",
                audience="cluster",
                plaintext_loader=lambda: canonical_json_bytes({
                    "api_key": self.credential_store.load_cmc_key(
                        credential_id,
                        selected_generation,
                    )
                }),
            )
            if secret_operation is not None:
                operation_ids.append(str(secret_operation["op_id"]))
                cas["secret_parent_generation"] = int(secret_operation["parent_generation"])
                materialized = rebuild_materialized_state(self.cluster_root, write=False)

            desired = self._desired_state(materialized)
            entry = self._cmc_entry(desired, credential_id)
            current_catalog_generation = int(entry.get("catalog_generation", 0))
            if entry.get("conflicted") is True:
                raise CredentialPublicationError("CMC pool entry is conflicted")
            if current_catalog_generation > selected_generation:
                raise CredentialPublicationError("CMC pool entry is newer than the requested generation")
            if current_catalog_generation == selected_generation:
                if entry and str(entry.get("secret_id") or credential_id) != credential_id:
                    raise CredentialPublicationError("CMC pool generation points to another secret")
            else:
                quota_domain_id = str(extra_pool.get("quota_domain_id") or credential_id)
                pool_payload: dict[str, Any] = {
                    "key_id": credential_id,
                    "secret_id": credential_id,
                    "catalog_generation": selected_generation,
                    "parent_generation": current_catalog_generation,
                    "label": str(record.get("label") or ""),
                    "active": bool(record.get("active", True)),
                    "quota_domain_id": quota_domain_id,
                    **extra_pool,
                }
                pool_operation = append_operation(
                    self.cluster_root,
                    "UPSERT_CMC_POOL_ENTRY",
                    pool_payload,
                )
                operation_ids.append(str(pool_operation["op_id"]))
                cas["pool_parent_generation"] = current_catalog_generation
                materialized = rebuild_materialized_state(self.cluster_root, write=False)

            desired = self._desired_state(materialized)
            domain_id = str(
                self._cmc_entry(desired, credential_id).get("quota_domain_id")
                or extra_pool.get("quota_domain_id")
                or credential_id
            )
            authority_operation = self._ensure_cmc_authority(materialized, domain_id)
            if authority_operation is not None:
                cas["authority_parent_epoch"] = int(authority_operation["parent_epoch"])
                cas["authority_epoch"] = int(authority_operation["authority_epoch"])
                cas["authority_operation_id"] = str(authority_operation["op_id"])
                materialized = rebuild_materialized_state(self.cluster_root, write=False)

            state_operation = self._cmc_publication_state_operation(
                credential_id,
                selected_generation,
            )
            if state_operation is None or str(state_operation.get("state") or "") != target_state:
                state_operation = self._append_cmc_state(
                    self._desired_state(materialized),
                    credential_id,
                    target_state,
                    credential_generation=selected_generation,
                )
                if state_operation is not None:
                    operation_ids.append(str(state_operation["op_id"]))
                    cas["state_parent_generation"] = int(state_operation["parent_generation"])
                    cas["state_generation"] = int(state_operation["state_generation"])

            return {
                "status": "published" if operation_ids else "already_published",
                "kind": "cmc_api_key",
                "credential_id": credential_id,
                "generation": selected_generation,
                "audience": "cluster",
                "recipient_ids": [recipient.node_id for recipient in recipients],
                "recipient_count": len(recipients),
                "sealed_blob_hash": blob_hash,
                "operation_ids": operation_ids,
                "cas": cas,
                "quota_domain_id": domain_id,
            }

    def publish_tradfi(
        self,
        credential_id: str,
        generation: int | None = None,
    ) -> dict[str, Any]:
        """Publish the current TradFi profile to active masters only."""

        record = self.credential_store.get_tradfi(credential_id)
        selected_generation = self._selected_generation(record, generation)
        if selected_generation != int(record["generation"]):
            raise CredentialPublicationError("only the current TradFi generation can be published")

        with advisory_file_lock(self.paths.root / ".append_sequence"):
            keys, materialized = self._ensure_local_crypto_membership()
            recipients, membership_roles = self._audience_recipients(materialized, "masters")
            blob_hash, operation = self._ensure_secret_publication(
                materialized,
                keys,
                recipients,
                membership_roles,
                record,
                selected_generation,
                kind="tradfi_profile",
                audience="masters",
                plaintext_loader=lambda: canonical_json_bytes({
                    "provider": str(record["provider"]),
                    "credentials": self.credential_store.load_tradfi_credentials(credential_id),
                }),
            )
            operation_ids = [] if operation is None else [str(operation["op_id"])]
            cas = {} if operation is None else {
                "secret_parent_generation": int(operation["parent_generation"])
            }
            return {
                "status": "published" if operation_ids else "already_published",
                "kind": "tradfi_profile",
                "credential_id": credential_id,
                "generation": selected_generation,
                "audience": "masters",
                "recipient_ids": [recipient.node_id for recipient in recipients],
                "recipient_count": len(recipients),
                "sealed_blob_hash": blob_hash,
                "operation_ids": operation_ids,
                "cas": cas,
            }

    def set_cmc_authority(
        self,
        quota_domain_id: str,
        authority_node_id: str,
        *,
        expected_epoch: int | None = None,
    ) -> dict[str, Any]:
        """Transfer one quota domain to an eligible active master via signed CAS."""

        with advisory_file_lock(self.paths.root / ".append_sequence"):
            _keys, materialized = self._ensure_local_crypto_membership()
            nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
            target = nodes.get(str(authority_node_id)) if isinstance(nodes, dict) else None
            if (
                not isinstance(target, dict)
                or target.get("enabled", True) is False
                or target.get("state_replica", True) is False
                or str(target.get("role") or "") != "master"
                or int(target.get("credential_protocol_version") or 0) < 2
                or target.get("credential_capable") is not True
            ):
                raise CredentialPublicationError("CMC authority target must be an active protocol-v2 master")
            desired = self._desired_state(materialized)
            authorities = ((desired.get("cmc_pool") or {}).get("authorities") or {})
            current = authorities.get(str(quota_domain_id)) if isinstance(authorities, dict) else None
            if isinstance(current, dict) and current.get("conflicted") is True:
                raise CredentialPublicationError("CMC authority state is conflicted")
            parent = int((current or {}).get("authority_epoch") or 0)
            if expected_epoch is not None and int(expected_epoch) != parent:
                raise CredentialPublicationError("CMC authority epoch changed")
            if isinstance(current, dict) and str(current.get("authority_node_id") or "") == str(authority_node_id):
                return {
                    "status": "current",
                    "quota_domain_id": str(quota_domain_id),
                    "authority_node_id": str(authority_node_id),
                    "authority_epoch": parent,
                    "operation_id": str(current.get("op_id") or ""),
                }
            operation = append_operation(
                self.cluster_root,
                "SET_CMC_AUTHORITY",
                {
                    "quota_domain_id": str(quota_domain_id),
                    "authority_node_id": str(authority_node_id),
                    "authority_epoch": parent + 1,
                    "parent_epoch": parent,
                },
            )
            rebuild_materialized_state(self.cluster_root)
            return {
                "status": "transferred" if parent else "assigned",
                "quota_domain_id": str(quota_domain_id),
                "authority_node_id": str(authority_node_id),
                "authority_epoch": parent + 1,
                "operation_id": str(operation["op_id"]),
            }

    def set_tradfi_active_profile(
        self,
        provider: str,
        profile_id: str | None,
        *,
        expected_generation: int | None = None,
    ) -> dict[str, Any]:
        """Publish the exact active TradFi profile for one provider via signed CAS."""

        with advisory_file_lock(self.paths.root / ".append_sequence"):
            self._ensure_local_crypto_membership()
            desired = self._desired_state(rebuild_materialized_state(self.cluster_root, write=False))
            profiles = desired.get("tradfi_active_profiles")
            profiles = profiles if isinstance(profiles, dict) else {}
            current = profiles.get(str(provider)) if isinstance(profiles.get(str(provider)), dict) else {}
            if current.get("conflicted") is True:
                raise CredentialPublicationError("TradFi active-profile state is conflicted")
            parent = int(current.get("activation_generation") or 0)
            if expected_generation is not None and int(expected_generation) != parent:
                raise CredentialPublicationError("TradFi active-profile generation changed")
            current_id = current.get("profile_id")
            if current and current_id == profile_id:
                return {
                    "status": "current",
                    "provider": str(provider),
                    "profile_id": profile_id,
                    "activation_generation": parent,
                    "operation_id": str(current.get("op_id") or ""),
                }
            if profile_id is not None:
                secret = (desired.get("secrets") or {}).get(profile_id)
                if not isinstance(secret, dict) or str(secret.get("secret_kind") or "") != "tradfi_profile":
                    raise CredentialPublicationError("TradFi active profile is not published")
                if str(secret.get("provider") or "") != str(provider):
                    raise CredentialPublicationError("TradFi active profile provider mismatch")
            operation = append_operation(
                self.cluster_root,
                "SET_TRADFI_ACTIVE_PROFILE",
                {
                    "provider": str(provider),
                    "profile_id": profile_id,
                    "activation_generation": parent + 1,
                    "parent_generation": parent,
                },
            )
            rebuild_materialized_state(self.cluster_root)
            return {
                "status": "activated" if profile_id is not None else "cleared",
                "provider": str(provider),
                "profile_id": profile_id,
                "activation_generation": parent + 1,
                "operation_id": str(operation["op_id"]),
            }

    def rewrap(self, credential_id: str | None = None) -> dict[str, Any]:
        """Rewrap one or every current published secret for exact membership."""

        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        desired = self._desired_state(materialized)
        secrets = desired.get("secrets") if isinstance(desired.get("secrets"), dict) else {}
        selected = [credential_id] if credential_id is not None else sorted(secrets)
        if credential_id is not None and credential_id not in secrets:
            raise CredentialPublicationError("published credential was not found")
        results = [self._rewrap_one(str(secret_id)) for secret_id in selected]
        return {
            "status": "rewrapped" if any(item["status"] == "rewrapped" for item in results) else "current",
            "count": len(results),
            "rewrapped": sum(item["status"] == "rewrapped" for item in results),
            "items": results,
        }

    def rotate_local_keys(
        self,
        *,
        crash_hook: Callable[[str], None] | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        """Rotate local node keys, recover safely, then rewrap every secret."""

        operations = DurableOperationStore(self.credential_store.root)
        internal_id = f"rotation:{operation_id}" if operation_id else ""
        if internal_id:
            current = operations.begin(internal_id, "cluster_key_rotation_core")
            if current.get("status") == "complete" and isinstance(current.get("result"), dict):
                return dict(current["result"])
        rotation = rotate_local_node_keys(self.cluster_root, crash_hook=crash_hook)
        rewrap = self.rewrap()
        if crash_hook:
            crash_hook("secrets_rewrapped")
        complete_node_key_rotation(self.cluster_root)
        result = {"status": "rotated", "rotation": rotation, "rewrap": rewrap}
        if internal_id:
            operations.complete(internal_id, result)
        return result

    def _rewrap_one(self, credential_id: str) -> dict[str, Any]:
        """Append one recipient-only CAS operation without rotating provider data."""

        with advisory_file_lock(self.paths.root / ".append_sequence"):
            keys, materialized = self._ensure_local_crypto_membership()
            desired = self._desired_state(materialized)
            secret = (desired.get("secrets") or {}).get(credential_id)
            if not isinstance(secret, dict):
                raise CredentialPublicationError("published credential was not found")
            if secret.get("conflicted") is True:
                raise CredentialPublicationError("credential recipient state is conflicted")
            kind = str(secret.get("secret_kind") or "")
            audience = str(secret.get("audience") or "")
            recipients, membership_roles = self._audience_recipients(materialized, audience)
            recipient_ids = [recipient.node_id for recipient in recipients]
            recipient_key_ids = self._recipient_key_ids(materialized, recipient_ids)
            membership_generation = int(
                (materialized.get("cluster_nodes") or {}).get(
                    "credential_membership_generation", 0
                )
            )
            if (
                int(secret.get("membership_generation") or 0) == membership_generation
                and list(secret.get("recipient_ids") or []) == recipient_ids
                and dict(secret.get("recipient_key_ids") or {}) == recipient_key_ids
            ):
                return {
                    "status": "current",
                    "credential_id": credential_id,
                    "generation": int(secret.get("generation") or 0),
                    "recipient_generation": int(secret.get("recipient_generation") or 1),
                }

            generation = int(secret.get("generation") or 0)
            context = SecretContext(
                cluster_id=str(desired["cluster_id"]),
                secret_id=credential_id,
                kind=kind,
                generation=generation,
                audience=audience,
            )
            if kind == "cmc_api_key":
                plaintext = canonical_json_bytes({
                    "api_key": self.credential_store.load_cmc_key(credential_id, generation)
                })
            elif kind == "tradfi_profile":
                record = self.credential_store.get_tradfi(credential_id)
                if int(record.get("generation") or 0) != generation:
                    raise CredentialPublicationError(
                        "current TradFi provider generation is unavailable locally"
                    )
                plaintext = canonical_json_bytes({
                    "provider": str(record["provider"]),
                    "credentials": self.credential_store.load_tradfi_credentials(credential_id),
                })
            else:
                raise CredentialPublicationError("unsupported published credential kind")

            identity = read_local_identity(self.cluster_root)
            envelope = seal_secret(
                plaintext,
                context,
                recipients,
                keys.signing_private_key,
                signer_id=str(identity["node_id"]),
            )
            validate_sealed_secret(
                envelope,
                keys.signing_public_key,
                expected_context=context,
                membership_roles=membership_roles,
            )
            raw = serialize_sealed_secret(envelope)
            blob_hash, blob_path, created = self._write_sealed_blob(raw)
            parent = int(secret.get("recipient_generation") or 1)
            payload = {
                "secret_id": credential_id,
                "provider_generation": generation,
                "recipient_generation": parent + 1,
                "parent_recipient_generation": parent,
                "membership_generation": membership_generation,
                "recipient_ids": recipient_ids,
                "recipient_key_ids": recipient_key_ids,
                "sealed_blob_hash": blob_hash,
            }
            try:
                operation = append_operation(
                    self.cluster_root,
                    "UPDATE_SECRET_RECIPIENTS",
                    payload,
                )
            except Exception:
                if created and not self._blob_is_referenced(blob_hash):
                    blob_path.unlink(missing_ok=True)
                raise
            return {
                "status": "rewrapped",
                "credential_id": credential_id,
                "generation": generation,
                "recipient_generation": parent + 1,
                "membership_generation": membership_generation,
                "recipient_ids": recipient_ids,
                "sealed_blob_hash": blob_hash,
                "operation_id": str(operation["op_id"]),
            }

    def publish_tombstone(self, secret_id: str, secret_kind: str) -> dict[str, Any]:
        """Idempotently tombstone one published secret and its CMC pool state."""

        if secret_kind not in _SECRET_KINDS:
            raise CredentialPublicationError(f"unsupported credential kind: {secret_kind}")
        with advisory_file_lock(self.paths.root / ".append_sequence"):
            self._ensure_local_crypto_membership()
            desired = self._desired_state(rebuild_materialized_state(self.cluster_root, write=False))
            current = (desired.get("secrets") or {}).get(secret_id) or {}
            tombstone = (desired.get("secret_tombstones") or {}).get(secret_id) or {}
            operation_ids: list[str] = []
            cas: dict[str, int] = {}
            if not tombstone or current:
                parent_generation = max(
                    int(current.get("generation") or 0),
                    int(tombstone.get("generation") or 0),
                )
                operation = append_operation(
                    self.cluster_root,
                    "TOMBSTONE_SECRET",
                    {
                        "secret_id": secret_id,
                        "secret_kind": secret_kind,
                        "generation": parent_generation + 1,
                        "parent_generation": parent_generation,
                    },
                )
                operation_ids.append(str(operation["op_id"]))
                cas["secret_parent_generation"] = parent_generation
                desired = self._desired_state(
                    rebuild_materialized_state(self.cluster_root, write=False)
                )

            if secret_kind == "cmc_api_key":
                state_operation = self._append_cmc_state(desired, secret_id, "tombstoned")
                if state_operation is not None:
                    operation_ids.append(str(state_operation["op_id"]))
                    cas["state_parent_generation"] = int(
                        state_operation["parent_generation"]
                    )
                    cas["state_generation"] = int(state_operation["state_generation"])

            final = self._desired_state(rebuild_materialized_state(self.cluster_root, write=False))
            final_tombstone = (final.get("secret_tombstones") or {}).get(secret_id) or {}
            return {
                "status": "tombstoned" if operation_ids else "already_tombstoned",
                "kind": secret_kind,
                "credential_id": secret_id,
                "generation": int(final_tombstone.get("generation") or 0),
                "operation_ids": operation_ids,
                "cas": cas,
            }

    def disable_cmc(self, credential_id: str) -> dict[str, Any]:
        """Idempotently publish the disabled state for one CMC pool key."""

        with advisory_file_lock(self.paths.root / ".append_sequence"):
            self._ensure_local_crypto_membership()
            desired = self._desired_state(rebuild_materialized_state(self.cluster_root, write=False))
            operation = self._append_cmc_state(desired, credential_id, "disabled")
            if operation is None:
                entry = self._cmc_entry(desired, credential_id)
                return {
                    "status": "already_disabled",
                    "kind": "cmc_api_key",
                    "credential_id": credential_id,
                    "state": "disabled",
                    "state_generation": int(entry.get("state_generation") or 0),
                    "operation_ids": [],
                    "cas": {},
                }
            return {
                "status": "disabled",
                "kind": "cmc_api_key",
                "credential_id": credential_id,
                "state": "disabled",
                "state_generation": int(operation["state_generation"]),
                "operation_ids": [str(operation["op_id"])],
                "cas": {
                    "state_parent_generation": int(operation["parent_generation"]),
                    "state_generation": int(operation["state_generation"]),
                },
            }

    def _ensure_local_crypto_membership(self) -> tuple[Any, dict[str, Any]]:
        """Sparsely add local v2 crypto fields without replacing node metadata."""

        identity = read_local_identity(self.cluster_root)
        keys = ensure_node_key_material(self.cluster_root)
        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
        node_id = str(identity["node_id"])
        current = nodes.get(node_id) if isinstance(nodes.get(node_id), dict) else None
        if current is not None and current.get("enabled", True) is False:
            raise CredentialPublicationError("the local cluster member is disabled")
        role = str((current or {}).get("role") or identity.get("role") or "master")
        bundle = keys.public_bundle(node_id, role)
        published_fields = {
            field: bundle[field]
            for field in CRYPTO_PUBLIC_FIELDS
        }
        published_fields.update({
            "credential_protocol_version": 2,
            "credential_capable": True,
        })
        if current is None or any(current.get(key) != value for key, value in published_fields.items()):
            payload: dict[str, Any] = {"node_id": node_id, **published_fields}
            if current is None:
                payload["role"] = role
            append_operation(
                self.cluster_root,
                "ADD_NODE" if current is None else "UPDATE_NODE",
                payload,
            )
            materialized = rebuild_materialized_state(self.cluster_root, write=False)
            nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
            current = nodes.get(node_id) if isinstance(nodes.get(node_id), dict) else None
        if current is None or any(current.get(key) != value for key, value in published_fields.items()):
            raise CredentialPublicationError("local crypto membership could not be materialized")
        return keys, materialized

    def _ensure_cmc_authority(
        self,
        materialized: dict[str, Any],
        quota_domain_id: str,
    ) -> dict[str, Any] | None:
        """Assign epoch one to the local eligible master on first publication."""

        desired = self._desired_state(materialized)
        authorities = ((desired.get("cmc_pool") or {}).get("authorities") or {})
        current = authorities.get(quota_domain_id) if isinstance(authorities, dict) else None
        if isinstance(current, dict):
            return None
        identity = read_local_identity(self.cluster_root)
        local_node_id = str(identity["node_id"])
        nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
        local = nodes.get(local_node_id) if isinstance(nodes, dict) else None
        if (
            not isinstance(local, dict)
            or local.get("enabled", True) is False
            or str(local.get("role") or "") != "master"
        ):
            raise CredentialPublicationError(
                "first CMC quota-domain publication requires an eligible local master authority"
            )
        return append_operation(
            self.cluster_root,
            "SET_CMC_AUTHORITY",
            {
                "quota_domain_id": quota_domain_id,
                "authority_node_id": local_node_id,
                "authority_epoch": 1,
                "parent_epoch": 0,
            },
        )

    def _audience_recipients(
        self,
        materialized: dict[str, Any],
        audience: str,
    ) -> tuple[list[SecretRecipient], dict[str, str]]:
        """Return the complete active audience or reject incomplete v2 crypto."""

        nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
        recipients: list[SecretRecipient] = []
        roles: dict[str, str] = {}
        for node_id in sorted(nodes):
            node = nodes[node_id]
            if not isinstance(node, dict):
                continue
            if node.get("enabled", True) is False or node.get("state_replica", True) is False:
                continue
            role = str(node.get("role") or "")
            if role not in {"master", "vps"}:
                continue
            missing = sorted(field for field in CRYPTO_PUBLIC_FIELDS if not node.get(field))
            try:
                protocol_version = int(node.get("credential_protocol_version"))
            except (TypeError, ValueError):
                protocol_version = 0
            if missing or node.get("credential_capable") is not True or protocol_version != 2:
                details = ", ".join(missing) if missing else "explicit protocol v2 capability"
                raise CredentialPublicationError(
                    f"active {role} member {node_id} lacks v2 crypto: {details}"
                )
            if audience == "masters" and role != "master":
                continue
            recipients.append(SecretRecipient(
                node_id=str(node_id),
                role=role,
                public_key=str(node["encryption_public_key"]),
            ))
            roles[str(node_id)] = role
        if not recipients:
            raise CredentialPublicationError(f"no active recipients for {audience} audience")
        return recipients, roles

    @staticmethod
    def _recipient_key_ids(
        materialized: dict[str, Any],
        recipient_ids: list[str],
    ) -> dict[str, str]:
        """Return exact public encryption registration IDs for recipients."""

        nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
        return {
            node_id: str((nodes.get(node_id) or {}).get("encryption_key_id") or "")
            for node_id in recipient_ids
        }

    def _ensure_secret_publication(
        self,
        materialized: dict[str, Any],
        keys: Any,
        recipients: list[SecretRecipient],
        membership_roles: dict[str, str],
        record: dict[str, Any],
        generation: int,
        *,
        kind: str,
        audience: str,
        plaintext_loader: Callable[[], bytes],
    ) -> tuple[str, dict[str, Any] | None]:
        """Create one sealed generation unless matching state already exists."""

        desired = self._desired_state(materialized)
        secret_id = str(record["id"])
        current = (desired.get("secrets") or {}).get(secret_id) or {}
        tombstone = (desired.get("secret_tombstones") or {}).get(secret_id) or {}
        if current.get("conflicted") is True:
            raise CredentialPublicationError("credential generation is conflicted")
        current_generation = int(current.get("generation") or 0)
        tombstone_generation = int(tombstone.get("generation") or 0)
        parent_generation = max(current_generation, tombstone_generation)
        context = SecretContext(
            cluster_id=str(desired["cluster_id"]),
            secret_id=secret_id,
            kind=kind,
            generation=generation,
            audience=audience,
        )
        if current_generation == generation and tombstone_generation < generation:
            if (
                str(current.get("secret_kind") or "") != kind
                or str(current.get("audience") or "") != audience
            ):
                raise CredentialPublicationError(
                    "credential generation exists with incompatible metadata"
                )
            blob_hash = str(current.get("sealed_blob_hash") or "")
            raw = self._read_sealed_blob(blob_hash)
            envelope = deserialize_sealed_secret(raw)
            signer_id = str(envelope.get("signer_id") or "")
            try:
                signing_key = membership_signing_public_key(
                    self.cluster_root,
                    signer_id,
                    str(envelope.get("signing_key_id") or ""),
                )
            except Exception as exc:
                raise CredentialPublicationError(
                    "published credential signer has no authenticated membership key"
                ) from exc
            validate_sealed_secret(
                envelope,
                signing_key,
                expected_context=context,
                membership_roles=membership_roles,
            )
            return blob_hash, None
        if generation <= parent_generation:
            raise CredentialPublicationError(
                "requested credential generation does not advance cluster state"
            )

        identity = read_local_identity(self.cluster_root)
        envelope = seal_secret(
            plaintext_loader(),
            context,
            recipients,
            keys.signing_private_key,
            signer_id=str(identity["node_id"]),
        )
        raw = serialize_sealed_secret(envelope)
        blob_hash, blob_path, created = self._write_sealed_blob(raw)
        metadata = {
            key: record[key]
            for key in ("label", "provider", "active", "shared")
            if key in record
        }
        if kind == "tradfi_profile":
            metadata["lifecycle_state"] = "pending" if record.get("pending") else "active"
        payload = {
            "secret_id": secret_id,
            "secret_kind": kind,
            "audience": audience,
            "generation": generation,
            "parent_generation": parent_generation,
            "sealed_blob_hash": blob_hash,
            "recipient_generation": 1,
            "parent_recipient_generation": 0,
            "membership_generation": int(
                ((materialized.get("cluster_nodes") or {}).get(
                    "credential_membership_generation", 0
                ))
            ),
            "recipient_ids": [recipient.node_id for recipient in recipients],
            "recipient_key_ids": self._recipient_key_ids(
                materialized,
                [recipient.node_id for recipient in recipients],
            ),
            **metadata,
        }
        try:
            operation = append_operation(self.cluster_root, "UPSERT_SECRET", payload)
        except Exception:
            if created and not self._blob_is_referenced(blob_hash):
                blob_path.unlink(missing_ok=True)
            raise
        return blob_hash, operation

    def _append_cmc_state(
        self,
        desired: dict[str, Any],
        credential_id: str,
        state: str,
        *,
        credential_generation: int | None = None,
    ) -> dict[str, Any] | None:
        """Append one CMC state CAS operation unless it is already current."""

        entry = self._cmc_entry(desired, credential_id)
        if entry.get("state") == "conflicted" or entry.get("state_conflicts"):
            raise CredentialPublicationError("CMC key state is conflicted")
        if credential_generation is None and str(entry.get("state") or "") == state:
            return None
        parent_generation = int(entry.get("state_generation") or 0)
        payload: dict[str, Any] = {
            "key_id": credential_id,
            "state": state,
            "state_generation": parent_generation + 1,
            "parent_generation": parent_generation,
        }
        if credential_generation is not None:
            payload["credential_generation"] = credential_generation
        return append_operation(self.cluster_root, "SET_CMC_KEY_STATE", payload)

    def _cmc_publication_state_operation(
        self,
        credential_id: str,
        credential_generation: int,
    ) -> dict[str, Any] | None:
        """Find a prior publisher state operation for one CMC generation."""

        matches = [
            operation
            for operation in load_operations(self.cluster_root)
            if operation.get("op") == "SET_CMC_KEY_STATE"
            and operation.get("key_id") == credential_id
            and operation.get("credential_generation") == credential_generation
        ]
        if not matches:
            return None
        return sorted(matches, key=lambda item: (int(item["state_generation"]), str(item["op_id"])))[-1]

    def _write_sealed_blob(self, raw: bytes) -> tuple[str, Path, bool]:
        """Atomically write one owner-only content-addressed envelope."""

        digest = hashlib.sha256(raw).hexdigest()
        blob_hash = f"sha256:{digest}"
        target = self.paths.sealed_blobs / "sha256" / digest[:2] / f"{digest}.json"
        ensure_private_directory_tree(self.paths.sealed_blobs, target.parent)
        if target.exists():
            secure_private_file(target)
            if target.read_bytes() != raw:
                raise CredentialPublicationError("sealed blob hash collision")
            return blob_hash, target, False
        atomic_write_private_bytes(target, raw)
        return blob_hash, target, True

    def _read_sealed_blob(self, blob_hash: str) -> bytes:
        """Read and verify one published owner-only sealed envelope."""

        target = self._sealed_blob_path(blob_hash)
        if not target.is_file() or target.is_symlink():
            raise CredentialPublicationError("published sealed credential blob is missing")
        secure_private_file(target)
        raw = target.read_bytes()
        if hashlib.sha256(raw).hexdigest() != blob_hash.removeprefix("sha256:"):
            raise CredentialPublicationError("published sealed credential blob hash mismatch")
        return raw

    def _sealed_blob_path(self, blob_hash: str) -> Path:
        """Resolve a validated sha256 reference below the sealed blob root."""

        digest = str(blob_hash).removeprefix("sha256:")
        if not str(blob_hash).startswith("sha256:") or len(digest) != 64:
            raise CredentialPublicationError("invalid sealed blob hash")
        try:
            int(digest, 16)
        except ValueError as exc:
            raise CredentialPublicationError("invalid sealed blob hash") from exc
        return self.paths.sealed_blobs / "sha256" / digest[:2] / f"{digest}.json"

    def _blob_is_referenced(self, blob_hash: str) -> bool:
        """Return whether an appended operation already references a sealed blob."""

        return any(
            operation.get("sealed_blob_hash") == blob_hash
            for operation in load_operations(self.cluster_root)
        )

    @staticmethod
    def _selected_generation(record: dict[str, Any], generation: int | None) -> int:
        """Validate a requested immutable local credential generation."""

        current = int(record.get("generation") or 0)
        selected = current if generation is None else generation
        if not isinstance(selected, int) or isinstance(selected, bool) or not 1 <= selected <= current:
            raise CredentialPublicationError("credential generation is unavailable")
        return selected

    @staticmethod
    def _desired_state(materialized: dict[str, Any]) -> dict[str, Any]:
        """Return the validated desired-state mapping from a materialized snapshot."""

        desired = materialized.get("desired_state")
        if not isinstance(desired, dict):
            raise CredentialPublicationError("cluster desired state is unavailable")
        return desired

    @staticmethod
    def _cmc_entry(desired: dict[str, Any], credential_id: str) -> dict[str, Any]:
        """Return one materialized CMC pool entry or an empty mapping."""

        pool = desired.get("cmc_pool") if isinstance(desired.get("cmc_pool"), dict) else {}
        entries = pool.get("entries") if isinstance(pool.get("entries"), dict) else {}
        entry = entries.get(credential_id)
        return entry if isinstance(entry, dict) else {}


__all__ = [
    "ClusterCredentialPublisher",
    "CredentialPublicationError",
]
