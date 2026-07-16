"""Best-effort CMC lease authority and signed transitive cluster mailbox."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import time
from typing import Any, Callable, Mapping
import uuid

from cluster_credentials import (
    ClusterCredentialError,
    ensure_node_key_material,
    sign_operation,
    verify_operation,
)
from file_lock import advisory_file_lock
from master.cluster_state import ClusterPaths, read_local_identity, rebuild_materialized_state
from secure_files import (
    atomic_write_private_text,
    ensure_private_directory,
    ensure_private_directory_tree,
    secure_private_file,
)


SERVICE = "CmcLeases"

MAILBOX_MESSAGE_TYPES = frozenset({
    "CMC_LEASE_REQUEST",
    "CMC_LEASE_GRANT",
    "CMC_LEASE_SETTLEMENT",
    "CMC_PROVIDER_EVENT",
    "CMC_LEASE_ACK",
})
MAX_MAILBOX_MESSAGE_BYTES = 64 * 1024
MAX_MAILBOX_MESSAGES = 2000
DEFAULT_MESSAGE_TTL = 3600
_AUTHORITY_VERSION = 2
_PROVIDER_VERSION = 1
_MICROCREDITS = 1_000_000
_FORBIDDEN_PAYLOAD_KEYS = frozenset({
    "api_key",
    "authorization",
    "credentials",
    "key",
    "password",
    "private_key",
    "secret",
    "token",
})


class CmcLeaseError(RuntimeError):
    """Raised when lease or mailbox state is invalid."""


class CmcLeaseAuthority:
    """Allocate small idempotent leases from one crash-safe local journal."""

    def __init__(
        self,
        state_root: Path | str,
        *,
        per_key_credit_limit: float = 100.0,
        per_key_request_limit: int = 100,
        max_lease_credits: float = 10.0,
        max_lease_requests: int = 1,
        lease_ttl: int = 300,
        minute_request_limit: int | None = None,
        daily_credit_limit: float | None = None,
        monthly_credit_limit: float = 10_000.0,
        concurrent_limit: int = 4,
        authority_epochs: Mapping[str, int] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Configure conservative per-key limits and short lease lifetime."""

        self.state_root = Path(os.path.abspath(Path(state_root).expanduser()))
        self.journal_path = self.state_root / "journal.json"
        self._lock_target = self.state_root / ".journal"
        self.per_key_credit_limit_micros = _credits_to_micros(per_key_credit_limit)
        self.per_key_request_limit = _positive_int(per_key_request_limit, "per_key_request_limit")
        self.max_lease_credits_micros = _credits_to_micros(max_lease_credits)
        self.max_lease_requests = _positive_int(max_lease_requests, "max_lease_requests")
        self.lease_ttl = _positive_int(lease_ttl, "lease_ttl")
        self.minute_request_limit = _positive_int(
            minute_request_limit if minute_request_limit is not None else per_key_request_limit,
            "minute_request_limit",
        )
        self.daily_credit_limit_micros = _credits_to_micros(
            daily_credit_limit if daily_credit_limit is not None else per_key_credit_limit
        )
        self.monthly_credit_limit_micros = _credits_to_micros(monthly_credit_limit)
        self.concurrent_limit = _positive_int(concurrent_limit, "concurrent_limit")
        self.authority_epochs = {
            _validate_identifier(domain_id, "quota_domain_id"): _positive_int(epoch, "authority_epoch")
            for domain_id, epoch in dict(authority_epochs or {}).items()
        }
        self._clock = clock
        self._prepare_root()

    def update_authority_epochs(self, authority_epochs: Mapping[str, int]) -> None:
        """Replace desired epochs so stale grants and settlements are rejected."""

        self.authority_epochs = {
            _validate_identifier(domain_id, "quota_domain_id"): _positive_int(epoch, "authority_epoch")
            for domain_id, epoch in dict(authority_epochs).items()
        }

    def grant(
        self,
        request_id: str,
        candidates: list[Mapping[str, Any]],
        *,
        recipient: str,
        estimated_credits: float | None = None,
        credits_micros: int | None = None,
        request_count: int = 1,
        quota_domain_id: str | None = None,
        authority_epoch: int | None = None,
    ) -> dict[str, Any] | None:
        """Bind a request to one exact lease and reserve its full budget once."""

        request_id = _validate_identifier(request_id, "request_id")
        recipient = _validate_identifier(recipient, "recipient")
        requested_credits = (
            _credits_to_nonnegative_micros(estimated_credits)
            if credits_micros is None
            else _nonnegative_int(credits_micros, "credits_micros")
        )
        requested_count = _positive_int(request_count, "request_count")
        now = int(self._clock())
        with self._locked():
            state = self._read_unlocked()
            self._expire_unlocked(state, now)
            existing = state["requests"].get(request_id)
            if isinstance(existing, dict):
                existing_lease = existing.get("lease")
                if isinstance(existing_lease, dict):
                    self._validate_lease_epoch(
                        existing_lease, quota_domain_id, authority_epoch
                    )
                self._write_unlocked(state)
                return deepcopy(existing["lease"])
            if (
                requested_credits > self.max_lease_credits_micros
                or requested_count > self.max_lease_requests
            ):
                self._write_unlocked(state)
                return None

            eligible: list[tuple[Mapping[str, Any], dict[str, Any], dict[str, Any], dict[str, int]]] = []
            for candidate in candidates:
                credential_id = str(candidate.get("id") or candidate.get("credential_id") or "")
                try:
                    credential_id = _validate_identifier(credential_id, "credential_id")
                    generation = _positive_int(candidate.get("generation"), "generation")
                    domain_id = _validate_identifier(
                        quota_domain_id or candidate.get("quota_domain_id") or credential_id,
                        "quota_domain_id",
                    )
                    requested_epoch = _positive_int(
                        authority_epoch
                        if authority_epoch is not None
                        else candidate.get("authority_epoch")
                        or self.authority_epochs.get(domain_id)
                        or 1,
                        "authority_epoch",
                    )
                except (CmcLeaseError, TypeError, ValueError):
                    continue
                current_epoch = self.authority_epochs.get(domain_id)
                if current_epoch is not None and requested_epoch != current_epoch:
                    raise CmcLeaseError("stale CMC authority epoch")
                limits = self._candidate_limits(candidate)
                domain = self._reconcile_domain_unlocked(
                    state, domain_id, requested_epoch, now
                )
                key = state["keys"].setdefault(
                    credential_id,
                    {
                        "generation": generation,
                        "reserved_credits_micros": 0,
                        "reserved_requests": 0,
                        "used_credits_micros": 0,
                        "used_requests": 0,
                    },
                )
                if int(key.get("generation") or 0) != generation:
                    if int(key.get("reserved_requests") or 0) > 0:
                        continue
                    key.update({
                        "generation": generation,
                        "reserved_credits_micros": 0,
                        "reserved_requests": 0,
                        "used_credits_micros": 0,
                        "used_requests": 0,
                    })
                total_credits = int(key["reserved_credits_micros"]) + int(key["used_credits_micros"])
                total_requests = int(key["reserved_requests"]) + int(key["used_requests"])
                if total_credits + requested_credits > self.per_key_credit_limit_micros:
                    continue
                if total_requests + requested_count > self.per_key_request_limit:
                    continue
                minute_requests = sum(int(slot["count"]) for slot in domain["minute_requests"])
                if minute_requests + requested_count > limits["minute_requests"]:
                    continue
                if (
                    int(domain["day_reserved_credits_micros"])
                    + int(domain["day_used_credits_micros"])
                    + requested_credits
                    > limits["daily_credits_micros"]
                ):
                    continue
                if (
                    int(domain["month_reserved_credits_micros"])
                    + int(domain["month_used_credits_micros"])
                    + requested_credits
                    > limits["monthly_credits_micros"]
                ):
                    continue
                if int(domain["concurrent_leases"]) + 1 > limits["concurrent_leases"]:
                    continue
                provider_remaining = domain.get("provider_remaining_micros")
                if provider_remaining is not None and int(provider_remaining) < requested_credits:
                    continue
                eligible.append((candidate, key, domain, limits))
            if not eligible:
                self._write_unlocked(state)
                return None

            candidate, key, domain, limits = min(
                eligible,
                key=lambda item: (
                    int(item[2]["day_reserved_credits_micros"])
                    + int(item[2]["day_used_credits_micros"]),
                    int(item[1]["reserved_credits_micros"]) + int(item[1]["used_credits_micros"]),
                    str(item[0].get("id") or item[0].get("credential_id") or ""),
                ),
            )
            credential_id = str(candidate.get("id") or candidate.get("credential_id"))
            generation = int(candidate.get("generation") or 0)
            domain_id = str(domain["quota_domain_id"])
            lease = {
                "lease_id": uuid.uuid4().hex,
                "request_id": request_id,
                "credential_id": credential_id,
                "secret_generation": generation,
                "quota_domain_id": domain_id,
                "authority_epoch": int(domain["authority_epoch"]),
                "recipient": recipient,
                "credits_micros": requested_credits,
                "request_count": requested_count,
                "granted_at": now,
                "expires_at": now + self.lease_ttl,
            }
            key["reserved_credits_micros"] = int(key["reserved_credits_micros"]) + requested_credits
            key["reserved_requests"] = int(key["reserved_requests"]) + requested_count
            domain["day_reserved_credits_micros"] += requested_credits
            domain["month_reserved_credits_micros"] += requested_credits
            domain["concurrent_leases"] += 1
            domain["minute_requests"].append({"at": now, "count": requested_count})
            domain["limits"] = limits
            state["requests"][request_id] = {"lease": lease}
            state["leases"][lease["lease_id"]] = {
                "request_id": request_id,
                "terminal": False,
            }
            self._write_unlocked(state)
            return deepcopy(lease)

    def settle(
        self,
        lease_id: str,
        *,
        outcome: str = "success",
        actual_credits: float | None = None,
        actual_credits_micros: int | None = None,
        status_code: int | None = None,
        quota_domain_id: str | None = None,
        authority_epoch: int | None = None,
        provider_status: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Terminally settle a lease once; duplicates return the first result."""

        lease_id = _validate_identifier(lease_id, "lease_id")
        now = int(self._clock())
        with self._locked():
            state = self._read_unlocked()
            self._expire_unlocked(state, now)
            lease_state = state["leases"].get(lease_id)
            if not isinstance(lease_state, dict):
                raise CmcLeaseError("unknown CMC lease")
            request = state["requests"].get(str(lease_state.get("request_id") or ""))
            lease = request.get("lease") if isinstance(request, dict) else None
            if not isinstance(lease, dict):
                raise CmcLeaseError("CMC lease request binding is missing")
            self._validate_lease_epoch(lease, quota_domain_id, authority_epoch)
            self._reconcile_domain_unlocked(
                state,
                str(lease["quota_domain_id"]),
                int(lease["authority_epoch"]),
                now,
            )
            existing = lease_state.get("settlement")
            if isinstance(existing, dict):
                self._write_unlocked(state)
                return deepcopy(existing)
            reserved = int(lease["credits_micros"])
            actual = (
                reserved
                if actual_credits is None and actual_credits_micros is None
                else _credits_to_nonnegative_micros(actual_credits)
                if actual_credits_micros is None
                else _nonnegative_int(actual_credits_micros, "actual_credits_micros")
            )
            settlement = {
                "lease_id": lease_id,
                "outcome": _bounded_text(outcome, "outcome"),
                "actual_credits_micros": actual,
                "status_code": int(status_code) if status_code is not None else None,
                "settled_at": now,
            }
            self._apply_terminal_unlocked(state, lease, settlement)
            self._apply_provider_snapshot_unlocked(
                state["domains"][str(lease["quota_domain_id"])],
                provider_status,
                now,
            )
            self._write_unlocked(state)
            return deepcopy(settlement)

    def record_provider_event(self, event_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Persist one idempotent non-secret provider event."""

        event_id = _validate_identifier(event_id, "event_id")
        clean = _validated_public_payload(payload)
        with self._locked():
            state = self._read_unlocked()
            existing = state["provider_events"].get(event_id)
            if isinstance(existing, dict):
                return deepcopy(existing)
            event = {"event_id": event_id, "payload": clean, "recorded_at": int(self._clock())}
            state["provider_events"][event_id] = event
            self._write_unlocked(state)
            return deepcopy(event)

    def status(self) -> dict[str, Any]:
        """Return secret-free durable authority counters."""

        with self._locked():
            state = self._read_unlocked()
            now = int(self._clock())
            self._expire_unlocked(state, now)
            for domain_id, domain in list(state["domains"].items()):
                self._reconcile_domain_unlocked(
                    state,
                    str(domain_id),
                    int(domain.get("authority_epoch") or 1),
                    now,
                )
            self._write_unlocked(state)
            return deepcopy(state)

    def _apply_terminal_unlocked(
        self,
        state: dict[str, Any],
        lease: dict[str, Any],
        settlement: dict[str, Any],
    ) -> None:
        """Move one reservation to terminal usage exactly once."""

        lease_state = state["leases"][str(lease["lease_id"])]
        if lease_state.get("terminal"):
            return
        key = state["keys"][str(lease["credential_id"])]
        key["reserved_credits_micros"] = max(
            0,
            int(key["reserved_credits_micros"]) - int(lease["credits_micros"]),
        )
        key["reserved_requests"] = max(
            0,
            int(key["reserved_requests"]) - int(lease["request_count"]),
        )
        key["used_credits_micros"] = int(key["used_credits_micros"]) + int(
            settlement["actual_credits_micros"]
        )
        key["used_requests"] = int(key["used_requests"]) + int(lease["request_count"])
        domain = state["domains"][str(lease["quota_domain_id"])]
        reserved = int(lease["credits_micros"])
        actual = int(settlement["actual_credits_micros"])
        domain["day_reserved_credits_micros"] = max(
            0, int(domain["day_reserved_credits_micros"]) - reserved
        )
        domain["month_reserved_credits_micros"] = max(
            0, int(domain["month_reserved_credits_micros"]) - reserved
        )
        domain["day_used_credits_micros"] += actual
        domain["month_used_credits_micros"] += actual
        domain["used_requests"] += int(lease["request_count"])
        domain["concurrent_leases"] = max(0, int(domain["concurrent_leases"]) - 1)
        if settlement.get("outcome") == "expired_assumed_used":
            domain["uncertain_credits_micros"] += actual
        lease_state["terminal"] = True
        lease_state["settlement"] = settlement

    def _expire_unlocked(self, state: dict[str, Any], now: int) -> None:
        """Conservatively consume expired unsettled reservations."""

        for lease_id, lease_state in state["leases"].items():
            if lease_state.get("terminal"):
                continue
            request = state["requests"].get(str(lease_state.get("request_id") or ""))
            lease = request.get("lease") if isinstance(request, dict) else None
            if not isinstance(lease, dict) or int(lease.get("expires_at") or 0) > now:
                continue
            settlement = {
                "lease_id": lease_id,
                "outcome": "expired_assumed_used",
                "actual_credits_micros": int(lease["credits_micros"]),
                "status_code": None,
                "settled_at": now,
            }
            self._apply_terminal_unlocked(state, lease, settlement)

    def _prepare_root(self) -> None:
        """Create and secure the authority root."""

        ensure_private_directory(self.state_root)
        if self.state_root.is_symlink() or self.journal_path.is_symlink():
            raise CmcLeaseError("refusing symlinked CMC lease authority state")

    def _locked(self):
        """Return the authority journal lock."""

        self._prepare_root()
        return advisory_file_lock(self._lock_target)

    def _read_unlocked(self) -> dict[str, Any]:
        """Read the single authority journal while locked."""

        if not self.journal_path.exists():
            return {
                "version": _AUTHORITY_VERSION,
                "requests": {},
                "leases": {},
                "keys": {},
                "provider_events": {},
                "domains": {},
            }
        secure_private_file(self.journal_path)
        try:
            state = json.loads(self.journal_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CmcLeaseError(f"unable to read CMC authority journal: {exc}") from exc
        if isinstance(state, dict) and state.get("version") == 1:
            state = self._migrate_v1_state(state)
        if not isinstance(state, dict) or state.get("version") != _AUTHORITY_VERSION:
            raise CmcLeaseError("unsupported CMC authority journal")
        for field in ("requests", "leases", "keys", "provider_events", "domains"):
            if not isinstance(state.get(field), dict):
                raise CmcLeaseError(f"CMC authority journal {field} must be an object")
        return state

    def _candidate_limits(self, candidate: Mapping[str, Any]) -> dict[str, int]:
        """Return quota limits, preferring desired provider-plan metadata."""

        def positive(value: Any, default: int) -> int:
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                return default
            return parsed if parsed > 0 else default

        def credits(value: Any, default: int) -> int:
            try:
                return _credits_to_micros(value)
            except CmcLeaseError:
                return default

        return {
            "minute_requests": positive(candidate.get("minute_limit"), self.minute_request_limit),
            "daily_credits_micros": credits(
                candidate.get("daily_limit"), self.daily_credit_limit_micros
            ),
            "monthly_credits_micros": credits(
                candidate.get("monthly_limit"), self.monthly_credit_limit_micros
            ),
            "concurrent_leases": positive(
                candidate.get("concurrent_limit"), self.concurrent_limit
            ),
        }

    def _reconcile_domain_unlocked(
        self,
        state: dict[str, Any],
        domain_id: str,
        authority_epoch: int,
        now: int,
    ) -> dict[str, Any]:
        """Initialize/reset one quota domain and prune its rolling minute window."""

        day = datetime.fromtimestamp(now, timezone.utc).date().isoformat()
        month = day[:7]
        domain = state["domains"].setdefault(
            domain_id,
            {
                "quota_domain_id": domain_id,
                "authority_epoch": authority_epoch,
                "day": day,
                "month": month,
                "day_reserved_credits_micros": 0,
                "day_used_credits_micros": 0,
                "month_reserved_credits_micros": 0,
                "month_used_credits_micros": 0,
                "minute_requests": [],
                "concurrent_leases": 0,
                "used_requests": 0,
                "uncertain_credits_micros": 0,
            },
        )
        if int(domain.get("authority_epoch") or 0) != authority_epoch:
            if int(domain.get("concurrent_leases") or 0) > 0:
                raise CmcLeaseError("cannot advance CMC authority epoch with active leases")
            domain["authority_epoch"] = authority_epoch
        if domain.get("day") != day:
            domain.update({
                "day": day,
                "day_reserved_credits_micros": 0,
                "day_used_credits_micros": 0,
            })
        if domain.get("month") != month:
            domain.update({
                "month": month,
                "month_reserved_credits_micros": 0,
                "month_used_credits_micros": 0,
            })
        domain["minute_requests"] = [
            slot
            for slot in domain.get("minute_requests", [])
            if isinstance(slot, dict) and int(slot.get("at") or 0) > now - 60
        ]
        if domain.get("provider_reset_at") is not None and int(domain["provider_reset_at"]) <= now:
            for field in (
                "provider_remaining_micros",
                "provider_limit_micros",
                "provider_used_micros",
                "provider_reset_at",
            ):
                domain.pop(field, None)
        return domain

    @staticmethod
    def _apply_provider_snapshot_unlocked(
        domain: dict[str, Any],
        provider_status: Mapping[str, Any] | None,
        now: int,
    ) -> None:
        """Persist bounded provider counters that can conservatively cap grants."""

        if not isinstance(provider_status, Mapping):
            return
        if any(field in provider_status for field in ("remaining_micros", "limit_micros", "used_micros")):
            snapshot = {
                field: int(provider_status[field])
                for field in ("remaining_micros", "limit_micros", "used_micros", "reset_at")
                if provider_status.get(field) is not None
            }
        else:
            snapshot = _safe_provider_status(provider_status)
        if not snapshot:
            return
        domain["provider_usage_snapshot"] = snapshot
        domain["provider_usage_updated_at"] = now
        for field in ("remaining_micros", "limit_micros", "used_micros", "reset_at"):
            if field in snapshot:
                domain[f"provider_{field}"] = snapshot[field]

    def _validate_lease_epoch(
        self,
        lease: Mapping[str, Any],
        quota_domain_id: str | None,
        authority_epoch: int | None,
    ) -> None:
        """Reject settlements addressed to an old quota-domain authority."""

        domain_id = str(lease.get("quota_domain_id") or "")
        lease_epoch = int(lease.get("authority_epoch") or 0)
        if quota_domain_id is not None and str(quota_domain_id) != domain_id:
            raise CmcLeaseError("CMC settlement quota domain mismatch")
        if authority_epoch is not None and int(authority_epoch) != lease_epoch:
            raise CmcLeaseError("stale CMC settlement authority epoch")
        current_epoch = self.authority_epochs.get(domain_id)
        if current_epoch is not None and current_epoch != lease_epoch:
            raise CmcLeaseError("stale CMC settlement authority epoch")

    def _migrate_v1_state(self, state: Mapping[str, Any]) -> dict[str, Any]:
        """Conservatively upgrade the pre-domain journal without losing leases."""

        migrated = deepcopy(dict(state))
        migrated["version"] = _AUTHORITY_VERSION
        migrated["domains"] = {}
        now = int(self._clock())
        for request in migrated.get("requests", {}).values():
            lease = request.get("lease") if isinstance(request, dict) else None
            if not isinstance(lease, dict):
                continue
            domain_id = str(lease.get("quota_domain_id") or lease.get("credential_id") or "")
            if not domain_id:
                continue
            lease.setdefault("authority_epoch", self.authority_epochs.get(domain_id, 1))
            domain = self._reconcile_domain_unlocked(
                migrated, domain_id, int(lease["authority_epoch"]), now
            )
            lease_state = migrated.get("leases", {}).get(str(lease.get("lease_id") or ""), {})
            if not lease_state.get("terminal"):
                reserved = int(lease.get("credits_micros") or 0)
                domain["day_reserved_credits_micros"] += reserved
                domain["month_reserved_credits_micros"] += reserved
                domain["concurrent_leases"] += 1
        return migrated

    def _write_unlocked(self, state: dict[str, Any]) -> None:
        """Atomically replace the single authority journal."""

        atomic_write_private_text(
            self.journal_path,
            json.dumps(state, indent=4, sort_keys=True) + "\n",
        )


class ClusterMailbox:
    """Store and validate signed non-secret messages for opaque relay."""

    def __init__(
        self,
        cluster_root: Path | str,
        *,
        clock: Callable[[], float] = time.time,
        membership_nodes: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        """Bind the mailbox to one cluster identity and membership snapshot."""

        self.cluster_root = Path(cluster_root)
        self.root = ClusterPaths.from_root(self.cluster_root).mailbox
        self.messages_root = self.root / "messages"
        self.ack_path = self.root / "acknowledgements.json"
        self._lock_target = self.root / ".mailbox"
        self._clock = clock
        self._membership_snapshot = (
            {
                str(node_id): deepcopy(dict(node))
                for node_id, node in membership_nodes.items()
                if isinstance(node, Mapping)
            }
            if membership_nodes is not None
            else None
        )
        self._prepare_root()

    @property
    def local_node_id(self) -> str:
        """Return the local cluster node ID."""

        return str(read_local_identity(self.cluster_root)["node_id"])

    def create_message(
        self,
        message_type: str,
        recipient: str,
        payload: Mapping[str, Any],
        *,
        message_id: str | None = None,
        created_at: int | None = None,
        expires_at: int | None = None,
        ttl: int = DEFAULT_MESSAGE_TTL,
    ) -> dict[str, Any]:
        """Create a signed message using the local operation-signing key."""

        message_type = _validate_message_type(message_type)
        recipient = _validate_identifier(recipient, "recipient")
        sender = self.local_node_id
        now = int(self._clock()) if created_at is None else _nonnegative_int(created_at, "created_at")
        expiry = now + _positive_int(ttl, "ttl") if expires_at is None else _positive_int(expires_at, "expires_at")
        if expiry <= now:
            raise CmcLeaseError("mailbox expires_at must be after created_at")
        message = {
            "sender": sender,
            "recipient": recipient,
            "message_id": _validate_identifier(message_id or uuid.uuid4().hex, "message_id"),
            "message_type": message_type,
            "created_at": now,
            "expires_at": expiry,
            "payload": _validated_public_payload(payload),
        }
        signed = sign_operation(
            message,
            ensure_node_key_material(self.cluster_root).signing_private_key,
            signer_id=sender,
        )
        self.validate_message(signed, allow_expired=False)
        return signed

    def validate_message(
        self,
        message: Mapping[str, Any],
        *,
        allow_expired: bool = False,
        membership_nodes: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Validate structure, public payload, membership signer, and signature."""

        if not isinstance(message, Mapping):
            raise CmcLeaseError("mailbox message must be an object")
        clean = dict(message)
        required = {
            "sender", "recipient", "message_id", "message_type", "created_at",
            "expires_at", "payload", "signature", "signer_id", "signing_key_id",
            "signature_version", "signature_algorithm",
        }
        if not required.issubset(clean):
            raise CmcLeaseError("mailbox message is missing required fields")
        sender = _validate_identifier(clean.get("sender"), "sender")
        recipient = _validate_identifier(clean.get("recipient"), "recipient")
        _validate_identifier(clean.get("message_id"), "message_id")
        _validate_message_type(str(clean.get("message_type") or ""))
        created_at = _nonnegative_int(clean.get("created_at"), "created_at")
        expires_at = _positive_int(clean.get("expires_at"), "expires_at")
        if expires_at <= created_at:
            raise CmcLeaseError("mailbox expires_at must be after created_at")
        if not allow_expired and expires_at <= int(self._clock()):
            raise CmcLeaseError("mailbox message has expired")
        _validated_public_payload(clean.get("payload"))
        if str(clean.get("signer_id") or "") != sender:
            raise CmcLeaseError("mailbox signer_id must match sender")
        nodes = membership_nodes if membership_nodes is not None else self._membership_nodes()
        sender_node = nodes.get(sender)
        recipient_node = nodes.get(recipient)
        if not _active_member(sender_node):
            raise CmcLeaseError("mailbox sender is not an active cluster member")
        if not _active_member(recipient_node):
            raise CmcLeaseError("mailbox recipient is not an active cluster member")
        public_key = str((sender_node or {}).get("signing_public_key") or "")
        if not public_key:
            raise CmcLeaseError("mailbox sender has no membership signing key")
        try:
            verify_operation(clean, public_key)
        except ClusterCredentialError as exc:
            raise CmcLeaseError(str(exc)) from exc
        encoded = json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")
        if len(encoded) > MAX_MAILBOX_MESSAGE_BYTES:
            raise CmcLeaseError("mailbox message is too large")
        return clean

    def put(self, message: Mapping[str, Any]) -> bool:
        """Persist one signed message idempotently before acknowledging delivery."""

        membership_nodes = self._membership_nodes()
        clean = self.validate_message(message, membership_nodes=membership_nodes)
        message_id = str(clean["message_id"])
        with self._locked():
            self._garbage_collect_unlocked(
                int(self._clock()),
                membership_nodes=membership_nodes,
            )
            path = self._message_path(message_id)
            if path.exists():
                existing = self._read_message_unlocked(
                    path,
                    allow_expired=True,
                    membership_nodes=membership_nodes,
                )
                if existing != clean:
                    raise CmcLeaseError("mailbox message_id already has different content")
                return False
            if len(list(self.messages_root.glob("*.json"))) >= MAX_MAILBOX_MESSAGES:
                raise CmcLeaseError("mailbox message limit reached")
            atomic_write_private_text(path, json.dumps(clean, indent=4, sort_keys=True) + "\n")
            return True

    def index(self) -> list[dict[str, Any]]:
        """Return a payload-free index of live messages."""

        with self._locked():
            if not any(self.messages_root.glob("*.json")):
                self._garbage_collect_unlocked(
                    int(self._clock()),
                    membership_nodes={},
                )
                return []
            membership_nodes = self._membership_nodes()
            self._garbage_collect_unlocked(
                int(self._clock()),
                membership_nodes=membership_nodes,
            )
            result = []
            for path in sorted(self.messages_root.glob("*.json")):
                message = self._read_message_unlocked(
                    path,
                    allow_expired=False,
                    membership_nodes=membership_nodes,
                )
                result.append({
                    key: message[key]
                    for key in (
                        "message_id", "message_type", "sender", "recipient",
                        "created_at", "expires_at",
                    )
                })
            return result

    def get(self, message_id: str) -> dict[str, Any]:
        """Return one live validated signed message."""

        message_id = _validate_identifier(message_id, "message_id")
        membership_nodes = self._membership_nodes()
        with self._locked():
            self._garbage_collect_unlocked(
                int(self._clock()),
                membership_nodes=membership_nodes,
            )
            path = self._message_path(message_id)
            if not path.is_file():
                raise CmcLeaseError("mailbox message not found")
            return self._read_message_unlocked(
                path,
                allow_expired=False,
                membership_nodes=membership_nodes,
            )

    def ack(self, message_id: str, node_id: str) -> bool:
        """Record durable possession and collect messages acknowledged by recipients."""

        message_id = _validate_identifier(message_id, "message_id")
        node_id = _validate_identifier(node_id, "node_id")
        membership_nodes = self._membership_nodes()
        if not _active_member(membership_nodes.get(node_id)):
            raise CmcLeaseError("mailbox acknowledgement node is not active")
        with self._locked():
            self._garbage_collect_unlocked(
                int(self._clock()),
                membership_nodes=membership_nodes,
            )
            if not self._message_path(message_id).is_file():
                return False
            acknowledgements = self._read_acks_unlocked()
            nodes = acknowledgements.setdefault(message_id, [])
            changed = node_id not in nodes
            if changed:
                nodes.append(node_id)
                nodes.sort()
                self._write_acks_unlocked(acknowledgements)
            self._garbage_collect_unlocked(
                int(self._clock()),
                membership_nodes=membership_nodes,
            )
            return changed

    def garbage_collect(self) -> dict[str, int]:
        """Remove expired and recipient-acknowledged messages."""

        membership_nodes = self._membership_nodes()
        with self._locked():
            return self._garbage_collect_unlocked(
                int(self._clock()),
                membership_nodes=membership_nodes,
            )

    def _garbage_collect_unlocked(
        self,
        now: int,
        *,
        membership_nodes: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, int]:
        """Collect mailbox files while holding the mailbox lock."""

        active_nodes = membership_nodes if membership_nodes is not None else self._membership_nodes()
        acknowledgements = self._read_acks_unlocked()
        expired = 0
        acknowledged = 0
        retained_ids: set[str] = set()
        invalid = 0
        for path in list(self.messages_root.glob("*.json")):
            try:
                message = self._read_message_unlocked(
                    path,
                    allow_expired=True,
                    membership_nodes=active_nodes,
                )
            except CmcLeaseError:
                path.unlink(missing_ok=True)
                invalid += 1
                continue
            message_id = str(message["message_id"])
            remove = False
            if int(message["expires_at"]) <= now:
                expired += 1
                remove = True
            elif str(message["recipient"]) in set(acknowledgements.get(message_id) or []):
                acknowledged += 1
                remove = True
            if remove:
                path.unlink(missing_ok=True)
                acknowledgements.pop(message_id, None)
            else:
                retained_ids.add(message_id)
        for message_id in list(acknowledgements):
            if message_id not in retained_ids:
                acknowledgements.pop(message_id, None)
        self._write_acks_unlocked(acknowledgements)
        return {"expired": expired, "acknowledged": acknowledged, "invalid": invalid}

    def _membership_nodes(self) -> dict[str, dict[str, Any]]:
        """Return current materialized membership for signature validation."""

        if self._membership_snapshot is not None:
            return self._membership_snapshot
        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
        return nodes if isinstance(nodes, dict) else {}

    def _prepare_root(self) -> None:
        """Create an owner-only mailbox tree."""

        ensure_private_directory_tree(self.root, self.messages_root)
        if self.root.is_symlink() or self.messages_root.is_symlink() or self.ack_path.is_symlink():
            raise CmcLeaseError("refusing symlinked cluster mailbox state")

    def _locked(self):
        """Return the mailbox transaction lock."""

        self._prepare_root()
        return advisory_file_lock(self._lock_target)

    def _message_path(self, message_id: str) -> Path:
        """Return a validated flat mailbox message path."""

        return self.messages_root / f"{_validate_identifier(message_id, 'message_id')}.json"

    def _read_message_unlocked(
        self,
        path: Path,
        *,
        allow_expired: bool,
        membership_nodes: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Read and validate one private mailbox file while locked."""

        secure_private_file(path)
        try:
            message = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CmcLeaseError(f"unable to read mailbox message: {exc}") from exc
        clean = self.validate_message(
            message,
            allow_expired=allow_expired,
            membership_nodes=membership_nodes,
        )
        if path != self._message_path(str(clean["message_id"])):
            raise CmcLeaseError("mailbox message path does not match message_id")
        return clean

    def _read_acks_unlocked(self) -> dict[str, list[str]]:
        """Read mailbox acknowledgement state."""

        if not self.ack_path.exists():
            return {}
        secure_private_file(self.ack_path)
        try:
            value = json.loads(self.ack_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CmcLeaseError(f"unable to read mailbox acknowledgements: {exc}") from exc
        if not isinstance(value, dict):
            raise CmcLeaseError("mailbox acknowledgements must be an object")
        return {
            str(message_id): [str(node_id) for node_id in nodes]
            for message_id, nodes in value.items()
            if isinstance(nodes, list)
        }

    def _write_acks_unlocked(self, value: dict[str, list[str]]) -> None:
        """Atomically persist mailbox acknowledgement state."""

        atomic_write_private_text(
            self.ack_path,
            json.dumps(value, indent=4, sort_keys=True) + "\n",
        )


class CmcLeaseProvider:
    """Adapt local or relayed authority leases to ``CmcPoolClient`` calls."""

    def __init__(
        self,
        mailbox: ClusterMailbox,
        authority_node_id: str,
        *,
        authority: CmcLeaseAuthority | None = None,
        authority_epoch: int = 1,
        quota_domain_id: str | None = None,
        authority_routes: Mapping[str, Mapping[str, Any]] | None = None,
        snapshot_loader: Callable[[], Mapping[str, Any] | None] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Configure the addressed authority and optional immediate local allocator."""

        self.mailbox = mailbox
        self.local_node_id = mailbox.local_node_id
        self.authority_node_id = _validate_identifier(authority_node_id, "authority_node_id")
        self.authority = authority
        self.authority_epoch = _positive_int(authority_epoch, "authority_epoch")
        self.quota_domain_id = (
            _validate_identifier(quota_domain_id, "quota_domain_id")
            if quota_domain_id is not None
            else None
        )
        self.authority_routes = _normalized_authority_routes(authority_routes or {})
        self._snapshot_loader = snapshot_loader
        self._clock = clock
        self._state_path = mailbox.root / "cmc_provider_state.json"
        self._lock_target = mailbox.root / ".cmc_provider_state"

    def acquire(
        self,
        candidates: list[Mapping[str, Any]],
        *,
        endpoint: str,
        estimated_credits: float,
    ) -> dict[str, str] | None:
        """Return an available grant or enqueue one request and soft-fallback."""

        try:
            self._refresh_authority_routes()
            self.process_inbox(refresh=False)
            safe_candidates = _safe_candidates(candidates)
            route, safe_candidates = self._route_candidates(safe_candidates)
            if route is None or not safe_candidates:
                return None
            credits_micros = _credits_to_nonnegative_micros(estimated_credits)
        except Exception:
            return None
        domain_id = str(route["quota_domain_id"])
        route_epoch = int(route["authority_epoch"])
        route_node = str(route["authority_node_id"])
        request_payload = {
            "endpoint": _bounded_text(str(endpoint).split("?", 1)[0], "endpoint"),
            "credits_micros": credits_micros,
            "request_count": 1,
            "quota_domain_id": domain_id,
            "authority_epoch": route_epoch,
            "candidates": safe_candidates,
        }
        fingerprint = sha256(
            json.dumps(request_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        request_id = uuid.uuid4().hex
        if route_node == self.local_node_id and self.authority is not None:
            lease = self.authority.grant(
                request_id,
                safe_candidates,
                recipient=self.local_node_id,
                credits_micros=credits_micros,
                quota_domain_id=domain_id,
                authority_epoch=route_epoch,
            )
            return _pool_lease(lease)

        try:
            with advisory_file_lock(self._lock_target):
                state = self._read_provider_state_unlocked()
                pending = state["pending"].get(fingerprint)
                if isinstance(pending, dict):
                    grant = state["grants"].get(str(pending.get("request_id") or ""))
                    if isinstance(grant, dict) and not grant.get("consumed"):
                        lease = grant.get("lease")
                        if _usable_lease(
                            lease,
                            safe_candidates,
                            int(self._clock()),
                            quota_domain_id=domain_id,
                            authority_epoch=route_epoch,
                        ):
                            grant["consumed"] = True
                            grant["consumed_at"] = int(self._clock())
                            state["pending"].pop(fingerprint, None)
                            self._write_provider_state_unlocked(state)
                            return _pool_lease(lease)
                        grant["consumed"] = True
                        grant["consumed_at"] = int(self._clock())
                        state["pending"].pop(fingerprint, None)
                        pending = None
                    message = pending.get("message") if isinstance(pending, dict) else None
                else:
                    message = None
                if not isinstance(pending, dict):
                    payload = {"request_id": request_id, **request_payload}
                    message = self.mailbox.create_message(
                        "CMC_LEASE_REQUEST",
                        route_node,
                        payload,
                    )
                    state["pending"][fingerprint] = {
                        "request_id": request_id,
                        "message": message,
                    }
                    self._write_provider_state_unlocked(state)
            if isinstance(message, dict):
                self.mailbox.put(message)
        except Exception:
            return None
        return None

    def settle(
        self,
        lease_token: str,
        *,
        status_code: int | None = None,
        error: str | None = None,
        provider_status: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Settle locally or durably queue one idempotent remote settlement."""

        lease_id = _validate_identifier(lease_token, "lease_token")
        outcome = _settlement_outcome(status_code, error, provider_status)
        try:
            self._refresh_authority_routes()
        except Exception:
            pass
        if self.authority is not None:
            try:
                return self.authority.settle(
                    lease_id,
                    outcome=outcome,
                    status_code=status_code,
                    provider_status=provider_status,
                )
            except CmcLeaseError as exc:
                if "unknown CMC lease" not in str(exc):
                    return None
        try:
            self.process_inbox()
            with advisory_file_lock(self._lock_target):
                state = self._read_provider_state_unlocked()
                record = state["settlements"].get(lease_id)
                if not isinstance(record, dict):
                    lease = next(
                        (
                            grant.get("lease")
                            for grant in state["grants"].values()
                            if isinstance(grant, dict)
                            and isinstance(grant.get("lease"), dict)
                            and str(grant["lease"].get("lease_id") or "") == lease_id
                        ),
                        None,
                    )
                    if not isinstance(lease, dict):
                        return None
                    domain_id = str(lease.get("quota_domain_id") or "")
                    epoch = int(lease.get("authority_epoch") or 0)
                    route = self._route_for_domain(domain_id)
                    if not route or int(route["authority_epoch"]) != epoch:
                        return None
                    message = self.mailbox.create_message(
                        "CMC_LEASE_SETTLEMENT",
                        str(route["authority_node_id"]),
                        {
                            "lease_id": lease_id,
                            "settlement_id": sha256(f"settlement:{lease_id}".encode()).hexdigest(),
                            "quota_domain_id": domain_id,
                            "authority_epoch": epoch,
                            "outcome": outcome,
                            "status_code": int(status_code) if status_code is not None else None,
                            "provider_status": _safe_provider_status(provider_status),
                        },
                    )
                    record = {"message": message}
                    state["settlements"][lease_id] = record
                    self._write_provider_state_unlocked(state)
                message = record["message"]
            self.mailbox.put(message)
            return {"queued": True, "lease_id": lease_id}
        except Exception:
            return None

    def process_inbox(self, *, refresh: bool = True) -> dict[str, int]:
        """Process only messages addressed to this node and leave relay traffic opaque."""

        if refresh:
            self._refresh_authority_routes()
        counts = {"processed": 0, "granted": 0, "settled": 0, "events": 0, "acked": 0}
        for item in self.mailbox.index():
            if str(item.get("recipient") or "") != self.local_node_id:
                continue
            message = self.mailbox.get(str(item["message_id"]))
            message_type = str(message["message_type"])
            payload = message["payload"]
            completed = False
            if message_type == "CMC_LEASE_REQUEST" and self.authority is not None:
                domain_id = str(payload.get("quota_domain_id") or "")
                route = self._route_for_domain(domain_id)
                if (
                    not route
                    or str(route.get("authority_node_id") or "") != self.local_node_id
                    or int(route.get("authority_epoch") or 0)
                    != int(payload.get("authority_epoch") or 0)
                ):
                    continue
                lease = self.authority.grant(
                    str(payload.get("request_id") or ""),
                    list(payload.get("candidates") or []),
                    recipient=str(message["sender"]),
                    credits_micros=int(payload.get("credits_micros") or 0),
                    request_count=int(payload.get("request_count") or 1),
                    quota_domain_id=domain_id,
                    authority_epoch=int(payload.get("authority_epoch") or 0),
                )
                if lease is None:
                    continue
                grant = self._outbox_message(
                    f"grant:{message['message_id']}",
                    "CMC_LEASE_GRANT",
                    str(message["sender"]),
                    {"request_id": str(payload["request_id"]), "lease": lease},
                    expires_at=int(lease["expires_at"]),
                )
                self.mailbox.put(grant)
                counts["granted"] += 1
                completed = True
            elif message_type == "CMC_LEASE_GRANT":
                lease = payload.get("lease")
                domain_id = str((lease or {}).get("quota_domain_id") or "")
                route = self._route_for_domain(domain_id)
                if (
                    not isinstance(lease, Mapping)
                    or not route
                    or str(route.get("authority_node_id") or "") != str(message["sender"])
                    or int(route.get("authority_epoch") or 0)
                    != int(lease.get("authority_epoch") or 0)
                ):
                    continue
                with advisory_file_lock(self._lock_target):
                    state = self._read_provider_state_unlocked()
                    request_id = str(payload.get("request_id") or "")
                    state["grants"].setdefault(
                        request_id,
                        {"lease": deepcopy(payload.get("lease")), "consumed": False},
                    )
                    self._write_provider_state_unlocked(state)
                completed = True
            elif message_type == "CMC_LEASE_SETTLEMENT" and self.authority is not None:
                self.authority.settle(
                    str(payload.get("lease_id") or ""),
                    outcome=str(payload.get("outcome") or "unknown"),
                    status_code=payload.get("status_code"),
                    quota_domain_id=str(payload.get("quota_domain_id") or ""),
                    authority_epoch=int(payload.get("authority_epoch") or 0),
                    provider_status=payload.get("provider_status"),
                )
                counts["settled"] += 1
                completed = True
            elif message_type == "CMC_PROVIDER_EVENT" and self.authority is not None:
                self.authority.record_provider_event(
                    str(payload.get("event_id") or ""),
                    payload,
                )
                counts["events"] += 1
                completed = True
            elif message_type == "CMC_LEASE_ACK":
                referenced = str(payload.get("message_id") or "")
                try:
                    original = self.mailbox.get(referenced)
                except CmcLeaseError:
                    original = None
                if (
                    isinstance(original, dict)
                    and str(original.get("sender") or "") == self.local_node_id
                    and str(original.get("recipient") or "") == str(message.get("sender") or "")
                ):
                    self.mailbox.ack(referenced, str(message["sender"]))
                counts["acked"] += 1
                completed = True
            if not completed:
                continue
            if message_type != "CMC_LEASE_ACK":
                ack = self._outbox_message(
                    f"ack:{message['message_id']}",
                    "CMC_LEASE_ACK",
                    str(message["sender"]),
                    {
                        "message_id": str(message["message_id"]),
                        "message_type": message_type,
                        "status": "processed",
                    },
                )
                self.mailbox.put(ack)
            self.mailbox.ack(str(message["message_id"]), self.local_node_id)
            counts["processed"] += 1
        return counts

    def _refresh_authority_routes(self) -> None:
        """Refresh desired authority routing and local epoch guards best-effort."""

        if self._snapshot_loader is None:
            return
        snapshot = self._snapshot_loader()
        desired = snapshot.get("desired_state") if isinstance(snapshot, Mapping) else None
        if not isinstance(desired, Mapping):
            desired = snapshot
        pool = desired.get("cmc_pool") if isinstance(desired, Mapping) else None
        routes = pool.get("authorities") if isinstance(pool, Mapping) else None
        if not isinstance(routes, Mapping):
            return
        normalized = _normalized_authority_routes(routes)
        if normalized:
            self.authority_routes = normalized
            first = normalized[sorted(normalized)[0]]
            self.authority_node_id = str(first["authority_node_id"])
            self.authority_epoch = int(first["authority_epoch"])
        if self.authority is not None:
            self.authority.update_authority_epochs({
                domain_id: int(route["authority_epoch"])
                for domain_id, route in normalized.items()
            })

    def _route_candidates(
        self,
        candidates: list[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """Choose one quota-domain route and keep only candidates sharing it."""

        grouped: dict[str, list[dict[str, Any]]] = {}
        for candidate in candidates:
            domain_id = str(
                candidate.get("quota_domain_id")
                or self.quota_domain_id
                or candidate["id"]
            )
            candidate["quota_domain_id"] = domain_id
            grouped.setdefault(domain_id, []).append(candidate)
        for domain_id in sorted(grouped):
            route = self.authority_routes.get(domain_id)
            if route is None and not self.authority_routes:
                route = {
                    "quota_domain_id": domain_id,
                    "authority_node_id": self.authority_node_id,
                    "authority_epoch": self.authority_epoch,
                }
            if route is not None:
                return dict(route), grouped[domain_id]
        return None, []

    def _route_for_domain(self, domain_id: str) -> dict[str, Any] | None:
        """Return desired routing or the legacy single-authority route."""

        route = self.authority_routes.get(domain_id)
        if route is not None:
            return route
        if self.authority_routes:
            return None
        return {
            "quota_domain_id": domain_id,
            "authority_node_id": self.authority_node_id,
            "authority_epoch": self.authority_epoch,
        }

    def _outbox_message(
        self,
        outbox_id: str,
        message_type: str,
        recipient: str,
        payload: Mapping[str, Any],
        *,
        expires_at: int | None = None,
    ) -> dict[str, Any]:
        """Return one durable exact signed outbox message."""

        key = sha256(outbox_id.encode("utf-8")).hexdigest()
        with advisory_file_lock(self._lock_target):
            state = self._read_provider_state_unlocked()
            existing = state["outbox"].get(key)
            if isinstance(existing, dict):
                return deepcopy(existing)
            message = self.mailbox.create_message(
                message_type,
                recipient,
                payload,
                message_id=key,
                expires_at=expires_at,
            )
            state["outbox"][key] = message
            self._write_provider_state_unlocked(state)
            return deepcopy(message)

    def _read_provider_state_unlocked(self) -> dict[str, Any]:
        """Read durable adapter request, grant, settlement, and outbox state."""

        if not self._state_path.exists():
            return {
                "version": _PROVIDER_VERSION,
                "pending": {},
                "grants": {},
                "settlements": {},
                "outbox": {},
            }
        secure_private_file(self._state_path)
        try:
            state = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CmcLeaseError(f"unable to read CMC lease provider state: {exc}") from exc
        if not isinstance(state, dict) or state.get("version") != _PROVIDER_VERSION:
            raise CmcLeaseError("unsupported CMC lease provider state")
        for field in ("pending", "grants", "settlements", "outbox"):
            if not isinstance(state.get(field), dict):
                raise CmcLeaseError(f"CMC lease provider {field} must be an object")
        return state

    def _write_provider_state_unlocked(self, state: dict[str, Any]) -> None:
        """Atomically persist adapter state without secrets."""

        atomic_write_private_text(
            self._state_path,
            json.dumps(state, indent=4, sort_keys=True) + "\n",
        )


def _safe_candidates(candidates: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return only lease-relevant non-secret candidate metadata."""

    result = []
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            continue
        credential_id = _validate_identifier(
            candidate.get("id") or candidate.get("credential_id"),
            "credential_id",
        )
        item = {
            "id": credential_id,
            "generation": _positive_int(candidate.get("generation"), "generation"),
        }
        for field in (
            "quota_domain_id",
            "origin",
            "provider_plan",
            "minute_limit",
            "daily_limit",
            "monthly_limit",
            "concurrent_limit",
        ):
            if candidate.get(field) not in {None, ""}:
                item[field] = (
                    _positive_int(candidate[field], field)
                    if field.endswith("_limit")
                    else _bounded_text(candidate[field], field)
                )
        if "shared" in candidate:
            item["shared"] = bool(candidate["shared"])
        result.append(item)
    return result


def _validated_public_payload(value: Any, *, field: str = "payload") -> Any:
    """Copy JSON-compatible public data while rejecting secret-bearing fields."""

    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            name = str(key)
            lowered = name.lower()
            if (
                lowered in _FORBIDDEN_PAYLOAD_KEYS
                or lowered.endswith(("_api_key", "_password", "_private_key", "_secret", "_token"))
            ):
                raise CmcLeaseError(f"mailbox {field} contains forbidden field {name}")
            result[name] = _validated_public_payload(item, field=f"{field}.{name}")
        return result
    if isinstance(value, list):
        return [_validated_public_payload(item, field=field) for item in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise CmcLeaseError(f"mailbox {field} contains unsupported value")


def _safe_provider_status(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return bounded numeric provider diagnostics without arbitrary text."""

    result: dict[str, Any] = {}
    for key in ("remaining", "limit", "used", "reset_at", "status", "error_code"):
        item = (value or {}).get(key)
        if item is None:
            continue
        if key == "status":
            result[key] = _bounded_text(str(item), key)
        elif key == "error_code":
            try:
                result[key] = int(item)
            except (TypeError, ValueError):
                continue
        else:
            try:
                number = float(item)
            except (TypeError, ValueError):
                continue
            if math.isfinite(number):
                result[f"{key}_micros" if key != "reset_at" else key] = (
                    int(round(number * _MICROCREDITS)) if key != "reset_at" else int(number)
                )
    return result


def _settlement_outcome(
    status_code: int | None,
    error: str | None,
    provider_status: Mapping[str, Any] | None,
) -> str:
    """Classify a settlement without serializing arbitrary error text."""

    status = str((provider_status or {}).get("status") or "").lower()
    try:
        error_code = int((provider_status or {}).get("error_code"))
    except (TypeError, ValueError):
        error_code = 0
    if status_code == 429 or error_code in {1008, 1011}:
        return "rate_limited"
    if error_code in {1001, 1002, 1003, 1004, 1005, 1006, 1007}:
        return "provider_disabled"
    if error_code in {1009, 1010}:
        return "exhausted"
    if status_code in {401, 403} or "invalid" in status:
        return "invalid"
    if status in {"rate_limited", "rate-limited", "cooldown"}:
        return "rate_limited"
    if status_code == 402 or status in {"exhausted", "credit_exhausted"}:
        return "exhausted"
    if error is not None or (status_code is not None and status_code >= 500):
        return "error"
    return "success"


def _pool_lease(lease: Mapping[str, Any] | None) -> dict[str, str] | None:
    """Convert an authority lease to the adapter contract used by CmcPoolClient."""

    if not isinstance(lease, Mapping):
        return None
    return {
        "credential_id": str(lease["credential_id"]),
        "lease_token": str(lease["lease_id"]),
    }


def _usable_lease(
    lease: Any,
    candidates: list[dict[str, Any]],
    now: int,
    *,
    quota_domain_id: str | None = None,
    authority_epoch: int | None = None,
) -> bool:
    """Return whether a delivered lease still matches one eligible key generation."""

    if not isinstance(lease, Mapping) or int(lease.get("expires_at") or 0) <= now:
        return False
    if quota_domain_id is not None and str(lease.get("quota_domain_id") or "") != quota_domain_id:
        return False
    if authority_epoch is not None and int(lease.get("authority_epoch") or 0) != authority_epoch:
        return False
    return any(
        str(candidate["id"]) == str(lease.get("credential_id") or "")
        and int(candidate["generation"]) == int(lease.get("secret_generation") or 0)
        for candidate in candidates
    )


def _normalized_authority_routes(
    routes: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Validate non-conflicted desired quota-domain authority routes."""

    result: dict[str, dict[str, Any]] = {}
    for domain_id, raw in routes.items():
        if not isinstance(raw, Mapping) or raw.get("conflicted"):
            continue
        try:
            domain = _validate_identifier(
                raw.get("quota_domain_id") or domain_id, "quota_domain_id"
            )
            result[domain] = {
                "quota_domain_id": domain,
                "authority_node_id": _validate_identifier(
                    raw.get("authority_node_id"), "authority_node_id"
                ),
                "authority_epoch": _positive_int(
                    raw.get("authority_epoch"), "authority_epoch"
                ),
            }
        except CmcLeaseError:
            continue
    return result


def _active_member(value: Any) -> bool:
    """Return whether a materialized membership row may use the mailbox."""

    return (
        isinstance(value, dict)
        and value.get("enabled", True) is not False
        and value.get("state_replica", True) is not False
    )


def _validate_message_type(value: str) -> str:
    """Validate a supported mailbox message type."""

    if value not in MAILBOX_MESSAGE_TYPES:
        raise CmcLeaseError("unsupported CMC mailbox message type")
    return value


def _validate_identifier(value: Any, field: str) -> str:
    """Validate a bounded filesystem-safe protocol identifier."""

    text = _bounded_text(value, field)
    if text in {".", ".."} or "/" in text or "\\" in text or "\x00" in text:
        raise CmcLeaseError(f"invalid {field}")
    return text


def _bounded_text(value: Any, field: str, *, maximum: int = 512) -> str:
    """Validate bounded non-empty protocol text."""

    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum:
        raise CmcLeaseError(f"invalid {field}")
    if any(ord(character) < 0x20 or ord(character) == 0x7f for character in value):
        raise CmcLeaseError(f"invalid {field}")
    return value


def _positive_int(value: Any, field: str) -> int:
    """Return a positive non-boolean integer."""

    if isinstance(value, bool):
        raise CmcLeaseError(f"{field} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise CmcLeaseError(f"{field} must be a positive integer") from exc
    if result < 1:
        raise CmcLeaseError(f"{field} must be a positive integer")
    return result


def _nonnegative_int(value: Any, field: str) -> int:
    """Return a nonnegative non-boolean integer."""

    if isinstance(value, bool):
        raise CmcLeaseError(f"{field} must be a nonnegative integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise CmcLeaseError(f"{field} must be a nonnegative integer") from exc
    if result < 0:
        raise CmcLeaseError(f"{field} must be a nonnegative integer")
    return result


def _credits_to_micros(value: Any) -> int:
    """Convert positive finite credits to canonical integer microcredits."""

    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise CmcLeaseError("credits must be a positive finite number") from exc
    if not math.isfinite(number) or number <= 0:
        raise CmcLeaseError("credits must be a positive finite number")
    return max(1, int(math.ceil(number * _MICROCREDITS)))


def _credits_to_nonnegative_micros(value: Any) -> int:
    """Convert nonnegative finite actual credits to integer microcredits."""

    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise CmcLeaseError("actual credits must be a nonnegative finite number") from exc
    if not math.isfinite(number) or number < 0:
        raise CmcLeaseError("actual credits must be a nonnegative finite number")
    return int(math.ceil(number * _MICROCREDITS))


__all__ = [
    "CmcLeaseAuthority",
    "CmcLeaseError",
    "CmcLeaseProvider",
    "ClusterMailbox",
    "MAILBOX_MESSAGE_TYPES",
    "MAX_MAILBOX_MESSAGE_BYTES",
]
