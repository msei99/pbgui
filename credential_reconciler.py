"""Idempotent activation and recovery for pending cluster credentials."""

from __future__ import annotations

import json
from pathlib import Path
import time
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from cluster_credential_publisher import ClusterCredentialPublisher
from cluster_sync_command import (
    _append_credential_migration_acks,
    _append_tradfi_projection_ack,
)
from credential_store import CredentialStore, credential_mutation_lock
from cmc_pool import CmcPoolClient
from master.cluster_state import default_cluster_root, rebuild_materialized_state
from pb7_api_keys import project_active_tradfi_profiles
from pbgui_purefunc import pb7dir


SERVICE = "CredentialReconciler"
CMC_KEY_INFO_URL = "https://pro-api.coinmarketcap.com/v1/key/info"


def _required_acks_current(materialized: dict[str, Any], secret_id: str) -> bool:
    """Return whether every current recipient ACKs exact provider and recipient state."""

    desired = materialized.get("desired_state") if isinstance(materialized, dict) else {}
    desired = desired if isinstance(desired, dict) else {}
    secret = (desired.get("secrets") or {}).get(secret_id)
    if not isinstance(secret, dict) or secret.get("conflicted") is True:
        return False
    membership_generation = int(
        (materialized.get("cluster_nodes") or {}).get("credential_membership_generation") or 0
    )
    acknowledgements = desired.get("credential_materialization_acks")
    acknowledgements = acknowledgements if isinstance(acknowledgements, dict) else {}
    for node_id in secret.get("recipient_ids") or []:
        ack = acknowledgements.get(str(node_id))
        if not isinstance(ack, dict):
            return False
        if (
            int(ack.get("membership_generation") or -1) != membership_generation
            or int((ack.get("credential_generations") or {}).get(secret_id) or 0)
            != int(secret.get("generation") or 0)
            or int((ack.get("recipient_generations") or {}).get(secret_id) or 0)
            != int(secret.get("recipient_generation") or 1)
        ):
            return False
    return bool(secret.get("recipient_ids"))


def _projection_acks_current(materialized: dict[str, Any]) -> bool:
    """Return whether every active master ACKs the exact active-profile projection."""

    desired = materialized.get("desired_state") if isinstance(materialized, dict) else {}
    desired = desired if isinstance(desired, dict) else {}
    profiles = desired.get("tradfi_active_profiles")
    profiles = profiles if isinstance(profiles, dict) else {}
    expected = {
        str(provider): int(item.get("activation_generation") or 0)
        for provider, item in profiles.items()
        if isinstance(item, dict) and not item.get("conflicted")
    }
    membership_generation = int(
        (materialized.get("cluster_nodes") or {}).get("credential_membership_generation") or 0
    )
    nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
    masters = {
        str(node_id)
        for node_id, node in nodes.items()
        if isinstance(node, dict)
        and node.get("enabled", True) is not False
        and node.get("state_replica", True) is not False
        and str(node.get("role") or "") == "master"
    }
    acknowledgements = desired.get("tradfi_projection_acks")
    acknowledgements = acknowledgements if isinstance(acknowledgements, dict) else {}
    return bool(masters) and all(
        isinstance(acknowledgements.get(node_id), dict)
        and int(acknowledgements[node_id].get("membership_generation") or -1)
        == membership_generation
        and acknowledgements[node_id].get("active_profile_generations", {}) == expected
        and str(acknowledgements[node_id].get("projection_status") or "") == "current"
        and int(acknowledgements[node_id].get("projection_applied_generation") or 0) > 0
        for node_id in masters
    )


def _rewrap_if_membership_changed(publisher: Any, credential_id: str) -> bool:
    """Rewrap one pending secret when the publisher detects audience churn."""

    if not hasattr(publisher, "rewrap"):
        return False
    result = publisher.rewrap(credential_id)
    return int(result.get("rewrapped") or 0) > 0


def _project_tradfi(store: CredentialStore, pending_profile_id: str | None = None) -> dict[str, Any]:
    """Project the initiating master's exact active/pending selection."""

    configured = str(pb7dir() or "").strip()
    if not configured or not Path(configured).is_dir():
        raise RuntimeError("PB7 directory is not configured")
    return project_active_tradfi_profiles(
        store,
        Path(configured) / "api-keys.json",
        pending_profile_id=pending_profile_id,
    )


def _snapshot_cmc_basis(api_key: str, *, timeout: float = 5.0) -> dict[str, Any]:
    """Fetch a redacted best-effort provider basis without exposing the key in errors."""

    checked_at = time.time()
    request = Request(
        CMC_KEY_INFO_URL,
        headers={"X-CMC_PRO_API_KEY": api_key, "Accept": "application/json"},
        method="GET",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError("provider response exceeded limit")
        payload = json.loads(raw.decode("utf-8"))
        status = payload.get("status") if isinstance(payload, dict) else None
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(status, dict) or not isinstance(data, dict):
            raise ValueError("provider response shape is invalid")
        error_code = int(status.get("error_code") or 0)
        result: dict[str, Any] = {
            "validation_status": "valid" if error_code == 0 else "invalid",
            "validation_checked_at": checked_at,
        }
        plan = data.get("plan") if isinstance(data.get("plan"), dict) else {}
        usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
        if plan.get("credit_limit_monthly") is not None:
            provider_limit = float(plan["credit_limit_monthly"])
            result["provider_limit"] = provider_limit
            if provider_limit.is_integer():
                result["monthly_limit"] = int(provider_limit)
        if plan.get("credit_limit_monthly_reset") is not None:
            result["provider_reset_at"] = plan["credit_limit_monthly_reset"]
        if data.get("plan") and isinstance(data.get("plan"), dict):
            name = str(plan.get("name") or plan.get("plan_name") or "").strip()
            if name:
                result["provider_plan"] = name
        month = usage.get("current_month") if isinstance(usage.get("current_month"), dict) else {}
        if month.get("credits_used") is not None:
            result["provider_used"] = float(month["credits_used"])
            if result.get("provider_limit") is not None:
                result["provider_remaining"] = max(
                    float(result["provider_limit"]) - float(result["provider_used"]),
                    0.0,
                )
        return result
    except (HTTPError, URLError, TimeoutError, OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        return {
            "validation_status": "unavailable",
            "validation_checked_at": checked_at,
            "validation_error_category": type(exc).__name__,
        }


def reconcile_pending_credentials(
    pbgdir: Path | str,
    *,
    store: CredentialStore | None = None,
    tradfi_projector: Callable[[CredentialStore, str | None], dict[str, Any]] | None = None,
    publisher: Any | None = None,
) -> dict[str, Any]:
    """Resume every pending CMC and TradFi mutation without exposing secrets."""

    root = Path(pbgdir)
    credential_store = store or CredentialStore(root / "data" / "credentials")
    active_publisher = publisher or ClusterCredentialPublisher(default_cluster_root(root), credential_store)
    cluster_root = Path(
        getattr(active_publisher, "cluster_root", default_cluster_root(root))
    )
    standalone_adapter = not hasattr(active_publisher, "cluster_root")
    projector = tradfi_projector or _project_tradfi
    results: list[dict[str, Any]] = []
    with credential_mutation_lock(credential_store.root):
        for record in credential_store.list_cmc(active_only=False):
            operation_id = str(record.get("pending_operation_id") or "")
            if not record.get("pending") or not operation_id:
                continue
            credential_id = str(record["id"])
            record = credential_store.get_cmc(credential_id)
            stage = credential_store.pending_stage("cmc", credential_id, operation_id)
            publication: dict[str, Any] = {"status": "current"}
            if stage == "stored":
                basis = _snapshot_cmc_basis(
                    credential_store.load_cmc_key(credential_id, int(record["generation"]))
                )
                CmcPoolClient(
                    credential_store=credential_store,
                    state_root=credential_store.root / "cmc_pool",
                ).record_provider_snapshot(credential_id, int(record["generation"]), basis)
                pool_metadata = {
                    key: basis[key]
                    for key in ("provider_plan", "monthly_limit")
                    if key in basis
                }
                publication = active_publisher.publish_cmc(
                    credential_id,
                    state="pending",
                    pool_metadata=pool_metadata,
                )
                record = credential_store.set_pending_stage(
                    "cmc", credential_id, operation_id, "published"
                )
                stage = "published"
            materialized = {}
            if not standalone_adapter:
                _append_credential_migration_acks(cluster_root)
                materialized = rebuild_materialized_state(cluster_root, write=False)
            if stage == "published" and (
                standalone_adapter
                or (
                    not _rewrap_if_membership_changed(active_publisher, credential_id)
                    and _required_acks_current(materialized, credential_id)
                )
            ):
                publication = (
                    active_publisher.publish_cmc(credential_id, state="active")
                    if record.get("active")
                    else active_publisher.disable_cmc(credential_id)
                )
                record = credential_store.set_pending_stage(
                    "cmc", credential_id, operation_id, "activated"
                )
                stage = "activated"
            if stage == "activated":
                record = credential_store.finalize_pending_mutation(
                    "cmc", credential_id, operation_id
                )
                stage = "complete"
            results.append({
                "kind": "cmc",
                "credential_id": credential_id,
                "operation_id": operation_id,
                "status": "active" if stage == "complete" else "pending",
                "stage": stage,
                "publication_status": publication.get("status"),
            })

        for record in credential_store.list_tradfi(active_only=False):
            operation_id = str(record.get("pending_operation_id") or "")
            if not record.get("pending") or not operation_id:
                continue
            profile_id = str(record["id"])
            record = credential_store.get_tradfi(profile_id)
            provider = str(record.get("provider") or "")
            stage = credential_store.pending_stage("tradfi", profile_id, operation_id)
            deleting = record.get("pending_delete") is True
            if deleting:
                if stage == "stored":
                    activation = (
                        active_publisher.set_tradfi_active_profile(provider, None)
                        if hasattr(active_publisher, "set_tradfi_active_profile")
                        else {"status": "cleared"}
                    )
                    tombstone = active_publisher.publish_tombstone(profile_id, "tradfi_profile")
                    record = credential_store.set_pending_stage(
                        "tradfi", profile_id, operation_id, "published"
                    )
                    stage = "published"
                else:
                    activation = {"status": "current"}
                    tombstone = {"status": "current"}
                projection = {"status": "current"}
                if stage == "published":
                    projection = projector(credential_store, None)
                    record = credential_store.set_pending_stage(
                        "tradfi", profile_id, operation_id, "projected"
                    )
                    stage = "projected"
                if stage == "projected":
                    if standalone_adapter:
                        projection_ready = True
                    else:
                        _append_tradfi_projection_ack(
                            cluster_root,
                            projection if projection.get("applied_generation") else None,
                        )
                        materialized = rebuild_materialized_state(cluster_root, write=False)
                        projection_ready = _projection_acks_current(materialized)
                    if projection_ready:
                        record = credential_store.set_pending_stage(
                            "tradfi", profile_id, operation_id, "activated"
                        )
                        stage = "activated"
                if stage == "activated":
                    credential_store.finalize_tradfi_delete(profile_id, operation_id)
                    stage = "complete"
                results.append({
                    "kind": "tradfi",
                    "credential_id": profile_id,
                    "operation_id": operation_id,
                    "status": "deleted" if stage == "complete" else "pending_delete",
                    "stage": stage,
                    "projection_status": projection.get("status"),
                    "activation_status": activation.get("status"),
                    "tombstone_status": tombstone.get("status"),
                })
                continue

            publication: dict[str, Any] = {"status": "current"}
            projection: dict[str, Any] = {"status": "current"}
            activation: dict[str, Any] = {"status": "pending"}
            if stage == "stored":
                publication = active_publisher.publish_tradfi(profile_id)
                record = credential_store.set_pending_stage(
                    "tradfi", profile_id, operation_id, "published"
                )
                stage = "published"
            if stage == "published":
                materialized = {}
                if not standalone_adapter:
                    _append_credential_migration_acks(cluster_root)
                    materialized = rebuild_materialized_state(cluster_root, write=False)
                recipients_ready = standalone_adapter or (
                    not _rewrap_if_membership_changed(active_publisher, profile_id)
                    and _required_acks_current(materialized, profile_id)
                )
                if recipients_ready:
                    projection = projector(
                        credential_store,
                        profile_id if record.get("active") else None,
                    )
                    record = credential_store.set_pending_stage(
                        "tradfi", profile_id, operation_id, "projected"
                    )
                    stage = "projected"
            if stage == "projected":
                activation = (
                    active_publisher.set_tradfi_active_profile(
                        provider,
                        profile_id if record.get("active") else None,
                    )
                    if hasattr(active_publisher, "set_tradfi_active_profile")
                    else {"status": "activated"}
                )
                record = credential_store.set_pending_stage(
                    "tradfi", profile_id, operation_id, "activated"
                )
                stage = "activated"
            if stage == "activated":
                if standalone_adapter:
                    projection_ready = True
                else:
                    _append_tradfi_projection_ack(
                        cluster_root,
                        projection if projection.get("applied_generation") else None,
                    )
                    materialized = rebuild_materialized_state(cluster_root, write=False)
                    projection_ready = _projection_acks_current(materialized)
                if projection_ready:
                    record = credential_store.finalize_pending_mutation(
                        "tradfi", profile_id, operation_id
                    )
                    stage = "complete"
            results.append({
                "kind": "tradfi",
                "credential_id": profile_id,
                "operation_id": operation_id,
                "status": "active" if stage == "complete" else "pending",
                "stage": stage,
                "publication_status": publication.get("status"),
                "projection_status": projection.get("status"),
                "activation_status": activation.get("status"),
            })
    return {
        "status": "pending" if any(item["status"].startswith("pending") for item in results) else "current",
        "items": results,
    }


__all__ = ["reconcile_pending_credentials"]
