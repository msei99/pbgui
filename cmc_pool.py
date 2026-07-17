"""Cross-process fair selection and accounting for local CMC credentials."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import time
from typing import Any, Callable, Mapping
import uuid

from credential_store import CredentialStore
from file_lock import advisory_file_lock
from master.cluster_state import local_cmc_credential_readiness, read_local_identity
from secure_files import (
    atomic_write_private_text,
    ensure_private_directory,
    ensure_private_directory_tree,
    secure_private_file,
)


SERVICE = "CmcPool"

_STATE_VERSION = 1
_MAX_ACQUISITION_HISTORY = 2000
_REMOTE_INELIGIBLE_STATES = frozenset({
    "conflicted",
    "disabled",
    "invalid",
    "provider-disabled",
    "provider_disabled",
    "tombstoned",
})


class CmcPoolExhaustedError(RuntimeError):
    """Raised when no active CMC credential can accept an acquisition."""


@dataclass(frozen=True)
class CmcAcquisition:
    """One independently accounted attempt using a selected CMC credential."""

    acquisition_id: str
    credential_id: str
    generation: int
    api_key: str = field(repr=False)
    endpoint: str
    estimated_credits: float
    acquired_at: float
    lease_token: str | None = field(default=None, repr=False)

    def metadata(self) -> dict[str, Any]:
        """Return acquisition diagnostics without the API key or lease token."""
        return {
            "acquisition_id": self.acquisition_id,
            "credential_id": self.credential_id,
            "generation": self.generation,
            "endpoint": self.endpoint,
            "estimated_credits": self.estimated_credits,
            "acquired_at": self.acquired_at,
            "leased": self.lease_token is not None,
        }


def estimate_endpoint_credits(
    endpoint: str,
    params: Mapping[str, Any] | None = None,
) -> float:
    """Estimate CMC call credits from endpoint cardinality and conversions."""
    path = "/" + str(endpoint or "").split("?", 1)[0].strip().strip("/").lower()
    values = dict(params or {})
    data_points = 1
    divisor = 100
    if path.endswith(("/listings/latest", "/listings/historical", "/market-pairs/latest")):
        data_points = _positive_int(values.get("limit"), 100)
        divisor = 200
    elif path.endswith(("/quotes/latest", "/quotes/historical", "/info")):
        data_points = max(
            _value_count(values.get("id")),
            _value_count(values.get("symbol")),
            _value_count(values.get("slug")),
            1,
        )
    elif path.endswith("/map"):
        data_points = _positive_int(values.get("limit"), 100)
    elif "limit" in values:
        data_points = _positive_int(values.get("limit"), 1)
    conversions = max(_value_count(values.get("convert")), 1)
    return float(max(math.ceil(data_points / divisor), 1) * conversions)


class CmcPoolClient:
    """Acquire CMC keys fairly and settle provider outcomes without doing HTTP."""

    def __init__(
        self,
        credential_store: CredentialStore | None = None,
        *,
        credentials_root: Path | str | None = None,
        state_root: Path | str | None = None,
        soft_credit_limit: float = 10_000,
        cooldown_seconds: float = 60,
        lease_provider: Any | None = None,
        desired_state_provider: Callable[[], Mapping[str, Any] | None] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Configure local state, soft per-key budget, cooldown, and optional leases."""
        if credential_store is not None and credentials_root is not None:
            raise ValueError("Pass credential_store or credentials_root, not both")
        self.store = credential_store or CredentialStore(credentials_root)
        self.soft_credit_limit = float(soft_credit_limit)
        self.cooldown_seconds = float(cooldown_seconds)
        if self.soft_credit_limit <= 0:
            raise ValueError("soft_credit_limit must be positive")
        if self.cooldown_seconds < 0:
            raise ValueError("cooldown_seconds cannot be negative")
        configured_state_root = state_root or (self.store.root / "cmc_pool")
        self.state_root = Path(
            os.path.abspath(Path(configured_state_root).expanduser())
        )
        self._state_path = self.state_root / "state.json"
        self._cluster_claim_path = self.state_root / "cluster_claim.json"
        self._lock_target = self.state_root / ".locks" / "state"
        self.lease_provider = lease_provider
        self.desired_state_provider = desired_state_provider
        self._clock = clock
        self._prepare_state_root()

    def estimate_credits(
        self,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
    ) -> float:
        """Estimate credits for an endpoint before acquiring a key."""
        return estimate_endpoint_credits(endpoint, params)

    def acquire(
        self,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        *,
        estimated_credits: float | None = None,
    ) -> CmcAcquisition:
        """Reserve budget and return one key for exactly one request attempt."""
        credits = float(
            self.estimate_credits(endpoint, params)
            if estimated_credits is None
            else estimated_credits
        )
        if not math.isfinite(credits) or credits < 0:
            raise ValueError("estimated_credits must be a non-negative finite number")
        desired_metadata = self._reconcile_desired_catalog()
        self._process_lease_inbox()
        active = self.store.active_cmc_credentials()
        for credential in active:
            credential.update(desired_metadata.get(str(credential["id"]), {}))
        if not active:
            raise CmcPoolExhaustedError("No active CMC credentials are configured")
        now = float(self._clock())
        day = self._day(now)
        lease_token: str | None = None
        with self._locked():
            state = self._read_state_unlocked()
            candidates = self._eligible_candidates(state, active, credits, now, day)
            if not candidates:
                self._write_state_unlocked(state)
                raise CmcPoolExhaustedError("All active CMC credentials are exhausted or cooling down")
            leased_id, lease_token = self._preferred_lease(candidates, endpoint, credits)
            if leased_id is not None:
                selected = next(item for item in candidates if item["id"] == leased_id)
            else:
                selected = min(
                    candidates,
                    key=lambda item: (
                        state["keys"][item["id"]]["used_credits"] / self.soft_credit_limit,
                        state["keys"][item["id"]].get("last_selected_sequence", 0),
                        item["id"],
                    ),
                )
            key_state = state["keys"][selected["id"]]
            state["sequence"] = int(state.get("sequence", 0)) + 1
            key_state["used_credits"] += credits
            key_state["total_acquisitions"] += 1
            key_state["last_selected_at"] = now
            key_state["last_selected_sequence"] = state["sequence"]
            key_state["status"] = "active"
            acquisition_id = uuid.uuid4().hex
            state["acquisitions"][acquisition_id] = {
                "credential_id": selected["id"],
                "generation": int(selected["generation"]),
                "estimated_credits": credits,
                "acquired_at": now,
                "settled": False,
                "lease_token": lease_token,
            }
            self._trim_acquisitions(state)
            self._write_state_unlocked(state)
        return CmcAcquisition(
            acquisition_id=acquisition_id,
            credential_id=selected["id"],
            generation=int(selected["generation"]),
            api_key=selected["api_key"],
            endpoint=str(endpoint),
            estimated_credits=credits,
            acquired_at=now,
            lease_token=lease_token,
        )

    def settle(
        self,
        acquisition: CmcAcquisition,
        *,
        status_code: int | None = None,
        error: str | BaseException | None = None,
        provider_status: Mapping[str, Any] | None = None,
        actual_credits: float | None = None,
        retry_after: float | None = None,
    ) -> dict[str, Any]:
        """Settle one attempt and classify provider rate, exhaustion, or key failures."""
        if not isinstance(acquisition, CmcAcquisition):
            raise TypeError("acquisition must be a CmcAcquisition")
        now = float(self._clock())
        normalized_status = _normalized_provider_status(provider_status)
        newly_settled = False
        with self._locked():
            state = self._read_state_unlocked()
            stored = state["acquisitions"].get(acquisition.acquisition_id)
            if not isinstance(stored, dict) or stored.get("credential_id") != acquisition.credential_id:
                raise ValueError("Unknown CMC acquisition")
            key_state = state["keys"].get(acquisition.credential_id)
            if not isinstance(key_state, dict):
                raise ValueError("CMC acquisition key state no longer exists")
            if not stored.get("settled"):
                if actual_credits is not None:
                    actual = float(actual_credits)
                    if not math.isfinite(actual) or actual < 0:
                        raise ValueError("actual_credits must be a non-negative finite number")
                    key_state["used_credits"] = max(
                        0.0,
                        float(key_state["used_credits"])
                        + actual
                        - float(stored["estimated_credits"]),
                    )
                    stored["actual_credits"] = actual
                self._apply_provider_status(key_state, normalized_status)
                outcome = self._classify_outcome(status_code, error, normalized_status)
                self._apply_outcome(key_state, outcome, now, retry_after, normalized_status)
                stored["settled"] = True
                stored["settled_at"] = now
                stored["status_code"] = status_code
                stored["outcome"] = outcome
                self._write_state_unlocked(state)
                newly_settled = True
            result = self._public_key_state(acquisition.credential_id, key_state, now)
        if newly_settled:
            self._settle_lease(acquisition, status_code, error, normalized_status)
        return result

    def status(self) -> dict[str, Any]:
        """Return pool diagnostics and metadata without credential secrets."""
        desired_metadata = self._reconcile_desired_catalog()
        self._process_lease_inbox()
        catalog = {item["id"]: item for item in self.store.list_cmc(active_only=False)}
        now = float(self._clock())
        day = self._day(now)
        with self._locked():
            state = self._read_state_unlocked()
            changed = False
            for credential_id, record in catalog.items():
                if credential_id in desired_metadata:
                    record.update(desired_metadata[credential_id])
                if record.get("desired_present") is False:
                    continue
                before = deepcopy(state["keys"].get(credential_id))
                key_state = self._reconcile_key_state(state, record, day)
                changed = changed or before != key_state
            if changed:
                self._write_state_unlocked(state)
            keys = []
            for credential_id, record in sorted(catalog.items()):
                if (desired_metadata.get(credential_id) or {}).get("desired_present") is False:
                    continue
                key_state = state["keys"].get(credential_id)
                item = deepcopy_without_secrets(record)
                if key_state is not None:
                    item.update(self._public_key_state(credential_id, key_state, now))
                    item.pop("credential_id", None)
                keys.append(item)
            return {
                "soft_credit_limit": self.soft_credit_limit,
                "day": day,
                "active_credentials": sum(1 for item in catalog.values() if item["active"]),
                "keys": keys,
            }

    def record_provider_snapshot(
        self,
        credential_id: str,
        generation: int,
        snapshot: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist a redacted best-effort `/v1/key/info` basis snapshot."""

        record = self.store.get_cmc(credential_id)
        if int(record.get("generation") or 0) != int(generation):
            raise ValueError("CMC snapshot generation changed")
        now = float(self._clock())
        with self._locked():
            state = self._read_state_unlocked()
            key_state = self._reconcile_key_state(state, record, self._day(now))
            key_state.update({
                "validation_status": str(snapshot.get("validation_status") or "unavailable"),
                "validation_checked_at": float(snapshot.get("validation_checked_at") or now),
                "validation_error_category": str(snapshot.get("validation_error_category") or ""),
            })
            for field in (
                "provider_plan",
                "provider_remaining",
                "provider_limit",
                "provider_used",
                "provider_reset_at",
            ):
                if field in snapshot:
                    key_state[field] = snapshot[field]
            self._write_state_unlocked(state)
            return self._public_key_state(credential_id, key_state, now)

    def _eligible_candidates(
        self,
        state: dict[str, Any],
        active: list[dict[str, Any]],
        credits: float,
        now: float,
        day: str,
    ) -> list[dict[str, Any]]:
        """Reconcile active keys and return those that can accept this attempt."""
        candidates = []
        for credential in active:
            key_state = self._reconcile_key_state(state, credential, day)
            if key_state.get("invalid_generation") == int(credential["generation"]):
                continue
            if float(key_state.get("cooldown_until", 0)) > now:
                continue
            if float(key_state.get("exhausted_until", 0)) > now:
                continue
            remaining = key_state.get("provider_remaining")
            if remaining is not None and float(remaining) < credits:
                key_state["status"] = "exhausted"
                key_state["exhausted_until"] = self._next_day(now)
                continue
            if float(key_state["used_credits"]) + credits > self.soft_credit_limit:
                key_state["status"] = "exhausted"
                key_state["exhausted_until"] = self._next_day(now)
                continue
            candidates.append(credential)
        return candidates

    def _reconcile_key_state(
        self,
        state: dict[str, Any],
        credential: Mapping[str, Any],
        day: str,
    ) -> dict[str, Any]:
        """Initialize a key or reset daily and generation-scoped failure state."""
        credential_id = str(credential["id"])
        generation = int(credential["generation"])
        key_state = state["keys"].setdefault(
            credential_id,
            {
                "generation": generation,
                "day": day,
                "used_credits": 0.0,
                "total_acquisitions": 0,
                "total_failures": 0,
                "last_selected_sequence": 0,
                "cooldown_until": 0.0,
                "exhausted_until": 0.0,
                "provider_remaining": None,
                "provider_limit": None,
                "provider_used": None,
                "provider_reset_at": None,
                "status": "active",
            },
        )
        if key_state.get("day") != day:
            key_state.update(
                {
                    "day": day,
                    "used_credits": 0.0,
                    "exhausted_until": 0.0,
                    "provider_remaining": None,
                    "provider_limit": None,
                    "provider_used": None,
                    "provider_reset_at": None,
                    "status": "active",
                }
            )
        if int(key_state.get("generation", 0)) != generation:
            key_state.update(
                {
                    "generation": generation,
                    "invalid_generation": None,
                    "cooldown_until": 0.0,
                    "exhausted_until": 0.0,
                    "provider_remaining": None,
                    "status": "active",
                }
            )
        return key_state

    def _preferred_lease(
        self,
        candidates: list[dict[str, Any]],
        endpoint: str,
        credits: float,
    ) -> tuple[str | None, str | None]:
        """Prefer an optional external lease but always permit local fallback."""
        if self.lease_provider is None:
            return None, None
        safe_candidates = [
            {key: value for key, value in item.items() if key != "api_key"}
            for item in candidates
        ]
        try:
            acquire = getattr(self.lease_provider, "acquire", self.lease_provider)
            lease = acquire(safe_candidates, endpoint=endpoint, estimated_credits=credits)
        except Exception:
            return None, None
        if isinstance(lease, str):
            credential_id, token = lease, lease
        elif isinstance(lease, Mapping):
            credential_id = str(lease.get("credential_id") or "")
            token_value = lease.get("lease_token")
            token = str(token_value) if token_value is not None else credential_id
        else:
            return None, None
        eligible_ids = {item["id"] for item in candidates}
        if credential_id not in eligible_ids:
            return None, None
        return credential_id, token

    def _process_lease_inbox(self) -> None:
        """Give local grants, requests, and settlements one bounded best-effort turn."""

        process = getattr(self.lease_provider, "process_inbox", None)
        if process is None:
            return
        try:
            process()
        except Exception:
            return

    def _reconcile_desired_catalog(self) -> dict[str, dict[str, Any]]:
        """Apply exact desired generations while preserving standalone local pools."""

        records = {str(item["id"]): item for item in self.store.list_cmc(active_only=False)}
        claimed = self._cluster_metadata_was_claimed() or any(
            str(record.get("origin") or "") == "cluster" for record in records.values()
        )
        if self.desired_state_provider is None:
            return self._fail_closed_metadata(records) if claimed else {}
        try:
            snapshot = self.desired_state_provider()
        except Exception:
            return self._fail_closed_metadata(records) if claimed else {}
        desired = snapshot.get("desired_state") if isinstance(snapshot, Mapping) else None
        if not isinstance(desired, Mapping):
            desired = snapshot
        pool = desired.get("cmc_pool") if isinstance(desired, Mapping) else None
        entries = pool.get("entries") if isinstance(pool, Mapping) else None
        secrets = desired.get("secrets") if isinstance(desired, Mapping) else None
        authorities = pool.get("authorities") if isinstance(pool, Mapping) else None
        has_cluster_metadata = (
            isinstance(entries, Mapping)
            and isinstance(secrets, Mapping)
            and (
                bool(entries)
                or any(
                    isinstance(secret, Mapping)
                    and str(secret.get("secret_kind") or "") == "cmc_api_key"
                    for secret in secrets.values()
                )
            )
        )
        if has_cluster_metadata:
            self._record_cluster_metadata_claim()
            claimed = True
        if not isinstance(entries, Mapping) or not isinstance(secrets, Mapping) or not has_cluster_metadata:
            return self._fail_closed_metadata(records) if claimed else {}

        metadata: dict[str, dict[str, Any]] = {}
        strict_expected: set[str] | None = None
        try:
            cluster_root = self.store.root.parent / "cluster"
            node_id = str(read_local_identity(cluster_root)["node_id"])
            readiness = local_cmc_credential_readiness(dict(snapshot), node_id, records.values())
            if readiness.get("cluster_origin_metadata") is True:
                desired_active = (
                    ((snapshot.get("desired_state") or {}).get("cmc_pool") or {}).get("entries") or {}
                )
                strict_expected = {
                    str(entry.get("secret_id") or entry.get("key_id") or credential_id)
                    for credential_id, entry in desired_active.items()
                    if isinstance(entry, Mapping) and str(entry.get("state") or "").lower() == "active"
                }
                if readiness.get("credential_active") is not True:
                    strict_expected = set()
        except Exception:
            strict_expected = set() if entries or secrets else None
        desired_ids = {str(credential_id) for credential_id in entries}
        for credential_id, record in records.items():
            if credential_id in desired_ids:
                continue
            if record.get("active") is True:
                try:
                    self.store.update_cmc(credential_id, active=False)
                except Exception:
                    pass
            metadata[credential_id] = {
                "desired_state": "absent",
                "desired_generation": 0,
                "desired_eligible": False,
                "desired_present": False,
            }
        for credential_id, entry in entries.items():
            if not isinstance(entry, Mapping):
                continue
            record = records.get(str(credential_id))
            if record is None:
                continue
            secret_id = str(entry.get("secret_id") or entry.get("key_id") or credential_id)
            secret = secrets.get(secret_id)
            expected_generation = (
                int(secret.get("generation") or 0) if isinstance(secret, Mapping) else 0
            )
            state = str(entry.get("state") or "").strip().lower()
            eligible = (
                state == "active"
                and not entry.get("conflicted")
                and not entry.get("state_conflicts")
                and expected_generation > 0
                and int(record.get("generation") or 0) == expected_generation
                and (strict_expected is None or secret_id in strict_expected)
            )
            if state in _REMOTE_INELIGIBLE_STATES:
                eligible = False
            if bool(record.get("active")) != eligible:
                try:
                    record = self.store.update_cmc(str(credential_id), active=eligible)
                except Exception:
                    eligible = False
            item = {
                key: entry[key]
                for key in (
                    "quota_domain_id",
                    "provider_plan",
                    "minute_limit",
                    "daily_limit",
                    "monthly_limit",
                )
                if entry.get(key) not in {None, ""}
            }
            domain_id = str(item.get("quota_domain_id") or credential_id)
            authority = authorities.get(domain_id) if isinstance(authorities, Mapping) else None
            if isinstance(authority, Mapping) and not authority.get("conflicted"):
                item["authority_epoch"] = int(authority.get("authority_epoch") or 0)
            item["desired_state"] = state
            item["desired_generation"] = expected_generation
            item["desired_eligible"] = eligible
            item["desired_present"] = True
            metadata[str(credential_id)] = item
        return metadata

    def _fail_closed_metadata(
        self,
        records: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Make every record ineligible while claimed cluster state is unavailable."""

        metadata: dict[str, dict[str, Any]] = {}
        for credential_id, record in records.items():
            if record.get("active") is True:
                try:
                    self.store.update_cmc(credential_id, active=False)
                except Exception:
                    pass
            metadata[credential_id] = {
                "desired_state": "unknown",
                "desired_generation": 0,
                "desired_eligible": False,
                "cluster_state_unavailable": True,
            }
        return metadata

    def _cluster_metadata_was_claimed(self) -> bool:
        """Return the durable one-way cluster-origin claim bit."""

        self._assert_state_path(self._cluster_claim_path)
        if not self._cluster_claim_path.is_file() or self._cluster_claim_path.is_symlink():
            desired_path = self.store.root.parent / "cluster" / "desired_state.json"
            try:
                desired = json.loads(desired_path.read_text(encoding="utf-8"))
                secrets = desired.get("secrets") if isinstance(desired, dict) else None
                entries = ((desired.get("cmc_pool") or {}).get("entries")) if isinstance(desired, dict) else None
                claimed = bool(entries) or any(
                    isinstance(secret, dict)
                    and str(secret.get("secret_kind") or "") == "cmc_api_key"
                    for secret in (secrets or {}).values()
                )
                if claimed:
                    atomic_write_private_text(
                        self._cluster_claim_path,
                        json.dumps({"cluster_metadata_claimed": True}, indent=4, sort_keys=True) + "\n",
                    )
                return claimed
            except (OSError, json.JSONDecodeError, AttributeError):
                if desired_path.exists():
                    atomic_write_private_text(
                        self._cluster_claim_path,
                        json.dumps({"cluster_metadata_claimed": True}, indent=4, sort_keys=True) + "\n",
                    )
                    return True
                return False
        try:
            payload = json.loads(self._cluster_claim_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return True
        return isinstance(payload, dict) and payload.get("cluster_metadata_claimed") is True

    def _record_cluster_metadata_claim(self) -> None:
        """Persist that this pool has participated in cluster-origin state."""

        if self._cluster_metadata_was_claimed():
            return
        atomic_write_private_text(
            self._cluster_claim_path,
            json.dumps({"cluster_metadata_claimed": True}, indent=4, sort_keys=True) + "\n",
        )

    def _settle_lease(
        self,
        acquisition: CmcAcquisition,
        status_code: int | None,
        error: str | BaseException | None,
        provider_status: Mapping[str, Any],
    ) -> None:
        """Best-effort settlement of an optional external lease."""
        if self.lease_provider is None or acquisition.lease_token is None:
            return
        settle = getattr(self.lease_provider, "settle", None)
        if settle is None:
            return
        try:
            settle(
                acquisition.lease_token,
                status_code=status_code,
                error=str(error) if error is not None else None,
                provider_status=dict(provider_status),
            )
        except Exception:
            return

    @staticmethod
    def _classify_outcome(
        status_code: int | None,
        error: str | BaseException | None,
        provider_status: Mapping[str, Any],
    ) -> str:
        """Classify provider feedback into pool failover states."""
        status_text = str(provider_status.get("status") or "").lower()
        error_text = str(error or "").lower()
        try:
            error_code = int(provider_status.get("error_code") or 0)
        except (TypeError, ValueError):
            error_code = 0
        if status_code == 429 or error_code in {1008, 1011}:
            return "rate_limited"
        if error_code in {1001, 1002, 1003, 1004, 1005, 1006, 1007}:
            return "provider_disabled"
        if error_code in {1009, 1010}:
            return "exhausted"
        if status_code in {401, 403} or "invalid" in status_text or "invalid api key" in error_text:
            return "invalid"
        if status_code == 429 or status_text in {"rate_limited", "rate-limited", "cooldown"}:
            return "rate_limited"
        if (
            status_code == 402
            or status_text in {"exhausted", "credit_exhausted"}
            or "credit limit" in error_text
            or "exhausted" in error_text
        ):
            return "exhausted"
        remaining = provider_status.get("remaining")
        if remaining is not None and float(remaining) <= 0:
            return "exhausted"
        if error is not None or (status_code is not None and status_code >= 500):
            return "error"
        return "success"

    def _apply_outcome(
        self,
        key_state: dict[str, Any],
        outcome: str,
        now: float,
        retry_after: float | None,
        provider_status: Mapping[str, Any],
    ) -> None:
        """Persist cooldown, exhaustion, invalidation, and success transitions."""
        key_state["last_outcome"] = outcome
        key_state["last_settled_at"] = now
        if outcome == "success":
            key_state["status"] = "active"
            key_state["consecutive_failures"] = 0
            return
        key_state["total_failures"] = int(key_state.get("total_failures", 0)) + 1
        key_state["consecutive_failures"] = int(key_state.get("consecutive_failures", 0)) + 1
        if outcome in {"invalid", "provider_disabled"}:
            key_state["status"] = outcome
            key_state["invalid_generation"] = int(key_state["generation"])
        elif outcome == "exhausted":
            key_state["status"] = "exhausted"
            reset = provider_status.get("reset_at")
            key_state["exhausted_until"] = max(
                _as_timestamp(reset, 0.0),
                self._next_day(now),
            )
        else:
            key_state["status"] = "cooldown"
            delay = self.cooldown_seconds if retry_after is None else max(float(retry_after), 0.0)
            key_state["cooldown_until"] = now + delay

    @staticmethod
    def _apply_provider_status(
        key_state: dict[str, Any],
        provider_status: Mapping[str, Any],
    ) -> None:
        """Persist normalized provider credit counters without secrets."""
        if provider_status.get("remaining") is not None:
            key_state["provider_remaining"] = max(float(provider_status["remaining"]), 0.0)
        if provider_status.get("limit") is not None:
            key_state["provider_limit"] = max(float(provider_status["limit"]), 0.0)
        if provider_status.get("used") is not None:
            key_state["provider_used"] = max(float(provider_status["used"]), 0.0)
        if provider_status.get("reset_at") is not None:
            key_state["provider_reset_at"] = provider_status["reset_at"]

    @staticmethod
    def _public_key_state(
        credential_id: str,
        key_state: Mapping[str, Any],
        now: float,
    ) -> dict[str, Any]:
        """Build secret-free status for one key."""
        return {
            "credential_id": credential_id,
            "generation": int(key_state.get("generation", 0)),
            "status": key_state.get("status", "unknown"),
            "used_credits": float(key_state.get("used_credits", 0)),
            "provider_remaining": key_state.get("provider_remaining"),
            "provider_limit": key_state.get("provider_limit"),
            "provider_used": key_state.get("provider_used"),
            "provider_reset_at": key_state.get("provider_reset_at"),
            "total_acquisitions": int(key_state.get("total_acquisitions", 0)),
            "total_failures": int(key_state.get("total_failures", 0)),
            "cooldown_remaining": max(float(key_state.get("cooldown_until", 0)) - now, 0.0),
            "exhausted_remaining": max(float(key_state.get("exhausted_until", 0)) - now, 0.0),
            "last_outcome": key_state.get("last_outcome"),
            "last_settled_at": key_state.get("last_settled_at"),
            "validation_status": key_state.get("validation_status", "unknown"),
            "validation_age_seconds": (
                max(now - float(key_state.get("validation_checked_at") or now), 0.0)
                if key_state.get("validation_checked_at") is not None
                else None
            ),
            "validation_error_category": key_state.get("validation_error_category", ""),
            "provider_plan": key_state.get("provider_plan"),
        }

    def _prepare_state_root(self) -> None:
        """Create owner-only state and lock directories without following symlinks."""
        if self.state_root.is_symlink():
            raise RuntimeError(f"Refusing symlinked CMC pool state: {self.state_root}")
        ensure_private_directory(self.state_root)
        ensure_private_directory_tree(self.state_root, self.state_root / ".locks")
        self._assert_state_path(self._state_path)
        self._assert_state_path(self._lock_target)
        lock_file = self._lock_target.with_name(f"{self._lock_target.name}.lock")
        if lock_file.is_symlink():
            raise RuntimeError(f"Refusing symlinked CMC pool lock: {lock_file}")

    def _assert_state_path(self, path: Path) -> Path:
        """Reject state paths outside the configured root or through symlinks."""
        path = Path(os.path.abspath(path))
        try:
            relative = path.relative_to(self.state_root)
        except ValueError as exc:
            raise RuntimeError(f"CMC pool state path is outside its root: {path}") from exc
        current = self.state_root
        if current.is_symlink():
            raise RuntimeError(f"Refusing symlinked CMC pool path: {current}")
        for part in relative.parts:
            if part in {"", ".", ".."}:
                raise RuntimeError(f"Invalid CMC pool path component: {part!r}")
            current = current / part
            if current.is_symlink():
                raise RuntimeError(f"Refusing symlinked CMC pool path: {current}")
        return path

    def _locked(self):
        """Return the process-safe state lock after revalidating the root."""
        self._prepare_state_root()
        return advisory_file_lock(self._lock_target)

    def _read_state_unlocked(self) -> dict[str, Any]:
        """Read persistent pool state while locked."""
        self._assert_state_path(self._state_path)
        if not self._state_path.exists():
            return {
                "version": _STATE_VERSION,
                "sequence": 0,
                "keys": {},
                "acquisitions": {},
            }
        secure_private_file(self._state_path)
        try:
            state = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unable to read CMC pool state: {exc}") from exc
        if not isinstance(state, dict) or state.get("version") != _STATE_VERSION:
            raise ValueError("Unsupported CMC pool state format")
        if not isinstance(state.get("keys"), dict) or not isinstance(state.get("acquisitions"), dict):
            raise ValueError("CMC pool state collections must be objects")
        return state

    def _write_state_unlocked(self, state: dict[str, Any]) -> None:
        """Atomically persist private pool state while locked."""
        self._assert_state_path(self._state_path)
        atomic_write_private_text(
            self._state_path,
            json.dumps(state, indent=4, sort_keys=True) + "\n",
        )

    @staticmethod
    def _trim_acquisitions(state: dict[str, Any]) -> None:
        """Bound persistent acquisition history while retaining recent settlements."""
        acquisitions = state["acquisitions"]
        excess = len(acquisitions) - _MAX_ACQUISITION_HISTORY
        if excess <= 0:
            return
        removable = sorted(
            (
                (float(value.get("acquired_at", 0)), key)
                for key, value in acquisitions.items()
                if value.get("settled")
            )
        )
        for _, acquisition_id in removable[:excess]:
            acquisitions.pop(acquisition_id, None)

    @staticmethod
    def _day(timestamp: float) -> str:
        """Return the UTC accounting day for a timestamp."""
        return datetime.fromtimestamp(timestamp, timezone.utc).date().isoformat()

    @staticmethod
    def _next_day(timestamp: float) -> float:
        """Return the start of the next UTC accounting day."""
        current = datetime.fromtimestamp(timestamp, timezone.utc)
        return datetime(
            current.year,
            current.month,
            current.day,
            tzinfo=timezone.utc,
        ).timestamp() + 86_400


def _positive_int(value: Any, default: int) -> int:
    """Coerce a positive integer used by endpoint credit estimates."""
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return default


def _value_count(value: Any) -> int:
    """Count comma-delimited or iterable request values."""
    if value is None or value == "":
        return 0
    if isinstance(value, str):
        return len([item for item in value.split(",") if item.strip()])
    if isinstance(value, (list, tuple, set)):
        return len(value)
    return 1


def _normalized_provider_status(status: Mapping[str, Any] | None) -> dict[str, Any]:
    """Normalize CMC headers or explicit status fields for settlement."""
    if not status:
        return {}
    lowered = {str(key).lower().replace("_", "-"): value for key, value in status.items()}
    aliases = {
        "remaining": ("remaining", "x-ratelimit-credit-remaining"),
        "limit": ("limit", "x-ratelimit-credit-limit"),
        "used": ("used", "x-ratelimit-credit-used"),
        "reset_at": ("reset-at", "x-ratelimit-credit-reset", "x-ratelimit-reset"),
        "status": ("status", "state"),
        "error_code": ("error-code", "error_code"),
    }
    result: dict[str, Any] = {}
    for canonical, names in aliases.items():
        for name in names:
            if name in lowered and lowered[name] not in {None, ""}:
                result[canonical] = lowered[name]
                break
    for numeric in ("remaining", "limit", "used", "error_code"):
        if numeric in result:
            try:
                result[numeric] = (
                    int(result[numeric]) if numeric == "error_code" else float(result[numeric])
                )
            except (TypeError, ValueError):
                result.pop(numeric)
    return result


def _as_timestamp(value: Any, default: float) -> float:
    """Parse an epoch or ISO provider reset timestamp."""
    if value in {None, ""}:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except ValueError:
            return default


def deepcopy_without_secrets(record: Mapping[str, Any]) -> dict[str, Any]:
    """Copy fixed catalog metadata while defensively dropping secret-like fields."""
    blocked = {"api_key", "key", "secret", "token", "password", "credentials"}
    return {key: value for key, value in record.items() if str(key).lower() not in blocked}
