"""Authenticated Profit Sweep policy, scheduler, and live-intent lifecycle."""

from __future__ import annotations

import asyncio
from decimal import Decimal, InvalidOperation, ROUND_DOWN
import hashlib
import json
from pathlib import Path
import time
import traceback
from typing import Any
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, StrictStr

from api.auth import SessionToken, require_auth
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR, PBGUI_SERIAL, PBGUI_VERSION
from profit_sweep import ProfitSweepStore, calculate_sweep, default_policy
from profit_sweep_exchanges import collect_readonly_snapshot, readonly_capability
from profit_sweep_transfers import (
    BITGET_TRANSFER_PERMISSION_REASON,
    BINANCE_TRANSFER_PERMISSION_REASON,
    browser_signing_request,
    prepare_transfer,
    reconcile_transfer,
    reverse_transfer_route,
    submit_browser_signed_transfer,
    submit_transfer,
    transfer_capability,
    verify_browser_signature,
)
from User import Users


SERVICE = "ProfitSweep"
router = APIRouter()
_VAULT_TEST_MINIMUM = Decimal("5")
_VAULT_STRICT_EPSILON = Decimal("0.000001")
_TEST_RECONCILE_ATTEMPTS = 10
_TEST_RECONCILE_DELAY_SECONDS = 1

_STORE: ProfitSweepStore | None = None
_SCHEDULER_TASK: asyncio.Task | None = None
_SCHEDULER_WAKE: asyncio.Event | None = None
_STOPPING = False
_EVALUATION_LOCKS: dict[str, asyncio.Lock] = {}
_ACTIVE_OPERATION_TASKS: set[asyncio.Task[Any]] = set()


class PolicyRequest(BaseModel):
    """Create or update one exchange-user Profit Sweep policy."""

    policy: dict[str, Any] = Field(default_factory=dict)
    expected_generation: int | None = None
    expected_policy_fingerprint: StrictStr | None = None
    confirmed_live_update: bool = False


class BaselineRequest(BaseModel):
    """Reset one policy baseline from an explicit cumulative PnL value."""

    cumulative_net_pnl: str
    expected_policy_fingerprint: StrictStr


class DeletePolicyRequest(BaseModel):
    """Bind destructive policy deletion to the reviewed policy revision."""

    expected_policy_fingerprint: StrictStr


class LiveActivationRequest(BaseModel):
    """Bind Live activation to the exact policy reviewed by the user."""

    expected_policy_fingerprint: StrictStr


class PreviewRequest(BaseModel):
    """Preview current form values without requiring a persisted policy."""

    policy: dict[str, Any] = Field(default_factory=dict)


class TestTransferRequest(BaseModel):
    """Request one server-owned manual internal-transfer roundtrip."""

    amount: StrictStr = Field(default="1")
    asset: StrictStr = Field(default="")
    operation_id: StrictStr = Field(default="")


class BrowserSignatureRequest(BaseModel):
    """Carry one foreground wallet signature without accepting signer metadata."""

    signature: StrictStr


def _store() -> ProfitSweepStore:
    """Return the lazily initialized private state store."""

    global _STORE
    if _STORE is None:
        _STORE = ProfitSweepStore(Path(PBGDIR) / "data" / "state" / "profit_sweep" / "profit_sweep.sqlite3")
    return _STORE


def _users() -> Users:
    """Load a fresh exchange-user catalog so credential changes are observed."""

    return Users()


def _user_or_404(user_name: str):
    """Resolve one exchange user without returning credential material."""

    user = _users().find_user(user_name)
    if user is None:
        raise _logged_http_error(
            404,
            "Exchange user not found",
            operation="resolve_user",
            user_name=user_name,
        )
    return user


def _safe_user(user: Any, policy: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return secret-free account metadata for the browser."""

    result = {
        "name": str(getattr(user, "name", "") or ""),
        "exchange": str(getattr(user, "exchange", "") or ""),
        "is_vault": bool(getattr(user, "is_vault", False)),
        "capability": readonly_capability(user),
        "has_policy": policy is not None,
    }
    if policy is not None:
        mode = policy["policy"]["operating_mode"]
        state_kind = "live" if mode in {"live", "paused_unknown"} else "simulation"
        state = policy[f"{state_kind}_state"]
        result.update({
            "operating_mode": mode,
            "generation": policy["generation"],
            "due": state["sweep_due"],
            "last_decision": state["last_decision"],
            "next_run_at": state["next_run_at"],
        })
    return result


def _logged_http_error(
    status_code: int,
    detail: str,
    *,
    operation: str,
    user_name: str | None = None,
    exc: Exception | None = None,
) -> HTTPException:
    """Log a bounded error before constructing its public HTTP exception."""

    meta: dict[str, Any] = {"operation": operation}
    if exc is not None:
        meta["error_type"] = type(exc).__name__
    _log(
        SERVICE,
        f"{operation} failed" + (f" for {user_name}" if user_name else "") + f": {detail}",
        level="WARNING",
        user=user_name,
        meta=meta,
    )
    return HTTPException(status_code=status_code, detail=detail)


def _decimal(value: Any, field: str) -> Decimal:
    """Parse one exchange amount as a finite Decimal."""

    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {field}") from exc
    if not parsed.is_finite():
        raise ValueError(f"Invalid {field}")
    return parsed


def _decimal_text(value: Decimal) -> str:
    """Render one Decimal without exponent notation."""

    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _event_hash(event_id: str) -> str:
    """Bound an exchange event identity to the store's validated hash field."""

    return hashlib.sha256(event_id.encode("utf-8")).hexdigest()


def _policy_fingerprint(policy: dict[str, Any]) -> str:
    """Return the stable fingerprint used for optimistic policy decisions."""

    encoded = json.dumps(
        policy,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _public_policy_record(record: dict[str, Any]) -> dict[str, Any]:
    """Attach a secret-free optimistic-concurrency token to one policy record."""

    return {**record, "policy_fingerprint": _policy_fingerprint(record["policy"])}


async def _run_owned_thread(function: Any, *args: Any, **kwargs: Any) -> Any:
    """Run one API-owned worker to completion even if its request is cancelled."""

    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    _ACTIVE_OPERATION_TASKS.add(task)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await asyncio.gather(task, return_exceptions=True)
        raise
    finally:
        _ACTIVE_OPERATION_TASKS.discard(task)


async def _run_account_operation(user_name: str, function: Any, *args: Any) -> Any:
    """Serialize one account's policy and real-funds lifecycle."""

    lock = _EVALUATION_LOCKS.setdefault(user_name, asyncio.Lock())
    async with lock:
        return await _run_owned_thread(function, *args)


def _jittered_delay(user_name: str, base_seconds: int, jitter_percent: str) -> int:
    """Return a stable per-user delay within the configured jitter range."""

    base = max(1, int(base_seconds))
    percent = max(Decimal("0"), _decimal(jitter_percent, "schedule jitter"))
    if percent == 0:
        return base
    digest = hashlib.sha256(user_name.encode("utf-8")).digest()
    ratio = Decimal(int.from_bytes(digest[:4], "big")) / Decimal(2**32 - 1)
    signed = ratio * Decimal("2") - Decimal("1")
    factor = Decimal("1") + signed * percent / Decimal("100")
    return max(1, int(Decimal(base) * factor))


def _persist_snapshot_events(store: ProfitSweepStore, user_name: str, exchange: str, snapshot: dict[str, Any]) -> None:
    """Persist normalized PnL-bearing snapshot events without storing raw responses."""

    asset_data = snapshot.get("asset") if isinstance(snapshot.get("asset"), dict) else {}
    settlement_asset = str(asset_data.get("symbol") or "").strip().upper()
    if not settlement_asset:
        raise ValueError("Exchange snapshot has no settlement asset")
    for event in snapshot.get("fills", {}).get("events", []):
        if not isinstance(event, dict) or event.get("time_ms") is None:
            continue
        store.upsert_ledger_event(
            user_name=user_name,
            exchange=exchange,
            event_time_ms=int(event["time_ms"]),
            event_hash=_event_hash(str(event.get("id") or "fill")),
            event_type="fill",
            asset=settlement_asset,
            realized_trade_pnl=str(event.get("closed_pnl") or "0"),
            fees=str(event.get("fee") or "0"),
            payload={
                "coin": str(event.get("coin") or ""),
                "trade_id": str(event.get("trade_id") or ""),
            },
        )
    for event in snapshot.get("funding", {}).get("events", []):
        if not isinstance(event, dict) or event.get("time_ms") is None:
            continue
        store.upsert_ledger_event(
            user_name=user_name,
            exchange=exchange,
            event_time_ms=int(event["time_ms"]),
            event_hash=_event_hash(str(event.get("id") or "funding")),
            event_type="funding",
            asset=settlement_asset,
            funding=str(event.get("amount") or "0"),
            payload={"coin": str(event.get("coin") or "")},
        )
    for attribution in snapshot.get("vault_leader_commissions", {}).get("attributions", []):
        if not isinstance(attribution, dict) or attribution.get("status") != "exact":
            continue
        event_time = attribution.get("time_ms")
        if event_time is None:
            continue
        store.upsert_ledger_event(
            user_name=user_name,
            exchange=exchange,
            event_time_ms=int(event_time),
            event_hash=_event_hash(str(attribution.get("commission_event_id") or "commission")),
            event_type="vaultLeaderCommission",
            asset=settlement_asset,
            exchange_corrections=str(attribution.get("amount") or "0"),
            payload={"status": "exact"},
        )


def _vault_transferable(policy: dict[str, Any], snapshot: dict[str, Any]) -> str:
    """Return a conservative leader-owned Vault cap for Dry simulation."""

    vault = snapshot.get("vault") if isinstance(snapshot.get("vault"), dict) else {}
    leader_equity = _decimal(vault.get("vault_equity") or "0", "vault equity")
    balances = vault.get("balances") if isinstance(vault.get("balances"), dict) else {}
    total_equity = _decimal(balances.get("account_value") or "0", "vault account value")
    shared_withdrawable = _decimal(balances.get("withdrawable") or "0", "vault withdrawable")
    max_withdrawable = _decimal(vault.get("max_withdrawable") or "0", "max withdrawable")
    retained = _decimal(policy.get("retained_leader_equity") or "100", "retained leader equity")
    fraction = _decimal(vault.get("leader_fraction") or "0", "leader fraction")
    minimum_fraction = Decimal("0.05") + _decimal(policy.get("share_safety_buffer") or "0", "share buffer")
    if minimum_fraction >= 1 or total_equity <= 0 or fraction < minimum_fraction:
        return "0"
    share_cap = max(
        Decimal("0"),
        (leader_equity - minimum_fraction * total_equity) / (Decimal("1") - minimum_fraction)
        - _VAULT_STRICT_EPSILON,
    )
    consistency = leader_equity / total_equity
    if abs(consistency - fraction) > Decimal("0.001"):
        return "0"
    reserve_fixed = _decimal(policy.get("vault_safety_reserve_amount") or "0", "vault reserve")
    reserve_percent = total_equity * _decimal(policy.get("vault_safety_reserve_percent") or "0", "vault reserve percent") / Decimal("100")
    reserve_mode = policy.get("vault_safety_reserve_mode")
    reserve = reserve_fixed if reserve_mode == "fixed" else reserve_percent if reserve_mode == "percent" else max(reserve_fixed, reserve_percent)
    caps = (
        max(Decimal("0"), leader_equity - retained - _VAULT_STRICT_EPSILON),
        share_cap,
        max_withdrawable,
        max(Decimal("0"), shared_withdrawable - reserve),
    )
    if bool(vault.get("closed")):
        return "0"
    if bool(vault.get("always_close_on_withdraw")) and (vault.get("positions") or vault.get("orders")):
        return "0"
    leader = snapshot.get("leader") if isinstance(snapshot.get("leader"), dict) else {}
    if policy.get("vault_destination") == "main_spot" and leader.get("account_mode") != "standard_manual":
        return "0"
    lockup = vault.get("lockup_until_ms")
    if lockup is not None and int(lockup) > int(time.time() * 1000):
        return "0"
    if policy.get("vault_withdraw_mode") == "flat_only" and (vault.get("positions") or vault.get("orders")):
        return "0"
    transferable = max(Decimal("0"), min(caps))
    if transferable < _VAULT_STRICT_EPSILON:
        return "0"
    return _decimal_text(transferable.quantize(_VAULT_STRICT_EPSILON, rounding=ROUND_DOWN))


def _snapshot_pnl_and_cap(
    store: ProfitSweepStore,
    policy_record: dict[str, Any],
    snapshot: dict[str, Any],
    *,
    cap_policy: dict[str, Any] | None = None,
) -> tuple[str, str]:
    """Derive cumulative policy PnL and a read-only transferable cap."""

    user_name = policy_record["user_name"]
    exchange = policy_record["exchange"]
    asset_data = snapshot.get("asset") if isinstance(snapshot.get("asset"), dict) else {}
    settlement_asset = str(asset_data.get("symbol") or "").strip().upper()
    if settlement_asset != str((cap_policy or policy_record["policy"]).get("asset") or "").strip().upper():
        raise ValueError("Exchange snapshot asset does not match the Profit Sweep policy")
    _persist_snapshot_events(store, user_name, exchange, snapshot)
    event_pnl = _decimal(store.ledger_net_pnl(user_name, exchange, settlement_asset), "ledger PnL")
    if snapshot.get("account_kind") == "vault":
        vault = snapshot.get("vault") if isinstance(snapshot.get("vault"), dict) else {}
        participant = _decimal(vault.get("all_time_pnl") or "0", "Vault all-time PnL")
        cap = _vault_transferable(cap_policy or policy_record["policy"], snapshot)
        # Leader commissions are already held in Main, not withdrawable from this Vault.
        return _decimal_text(participant), cap
    account = snapshot.get("account") if isinstance(snapshot.get("account"), dict) else {}
    return _decimal_text(event_pnl), str(account.get("withdrawable") or "0")


def _transient_snapshot_pnl_and_cap(policy: dict[str, Any], snapshot: dict[str, Any]) -> tuple[str, str]:
    """Derive a preview PnL/cap without creating policy or ledger rows."""

    if snapshot.get("account_kind") == "vault":
        vault = snapshot.get("vault") if isinstance(snapshot.get("vault"), dict) else {}
        participant = _decimal(vault.get("all_time_pnl") or "0", "Vault all-time PnL")
        return _decimal_text(participant), _vault_transferable(policy, snapshot)
    account = snapshot.get("account") if isinstance(snapshot.get("account"), dict) else {}
    realized = snapshot.get("realized_net_pnl")
    if realized is None:
        fill_total = sum(
            (
                _decimal(event.get("closed_pnl") or "0", "closed PnL")
                - _decimal(event.get("fee") or "0", "fee")
                for event in snapshot.get("fills", {}).get("events", [])
                if isinstance(event, dict)
            ),
            Decimal("0"),
        )
        funding_total = sum(
            (
                _decimal(event.get("amount") or "0", "funding")
                for event in snapshot.get("funding", {}).get("events", [])
                if isinstance(event, dict)
            ),
            Decimal("0"),
        )
        realized = _decimal_text(fill_total + funding_total)
    return str(realized or "0"), str(account.get("withdrawable") or "0")


def _history_window(
    policy_record: dict[str, Any],
    now_ms: int,
    *,
    state_kind: str = "simulation",
) -> tuple[int, int]:
    """Return an overlapping bounded history window for one evaluation."""

    state = policy_record[f"{state_kind}_state"]
    created_ms = int(policy_record["created_at"]) * 1000
    maximum_age_ms = int(policy_record["policy"]["maximum_history_age"]) * 1000
    earliest_ms = max(created_ms, now_ms - maximum_age_ms)
    last_scan = state.get("last_successful_scan_at")
    if last_scan is None:
        return earliest_ms, now_ms
    overlap_ms = 6 * 60 * 60 * 1000
    return max(earliest_ms, int(last_scan) * 1000 - overlap_ms), now_ms


def _evaluate_sync(user_name: str, *, commit: bool) -> dict[str, Any]:
    """Collect one complete read-only snapshot and evaluate its Dry policy."""

    store = _store()
    user = _user_or_404(user_name)
    policy_record = store.get_policy(user_name)
    if policy_record["exchange"] != str(user.exchange):
        raise ValueError("Policy exchange no longer matches the exchange user")
    if policy_record["policy"]["operating_mode"] != "dry":
        raise ValueError("Dry evaluation requires operating_mode=dry")
    now_ms = int(time.time() * 1000)
    since_ms, until_ms = _history_window(policy_record, now_ms)
    snapshot = collect_readonly_snapshot(
        user, since_ms, until_ms, 30.0, policy_record["policy"]["asset"]
    )
    if not snapshot.get("complete"):
        messages = ", ".join(str(item.get("code") or "read_error") for item in snapshot.get("errors", []))
        raise RuntimeError(f"Exchange snapshot incomplete: {messages or 'unknown'}")
    cumulative_pnl, max_transferable = _snapshot_pnl_and_cap(store, policy_record, snapshot)

    state = policy_record["simulation_state"]
    if commit and state.get("last_successful_scan_at") is None and policy_record["policy"]["baseline_mode"] == "from_enable":
        changes: dict[str, Any] = {}
        if _decimal(policy_record["policy"]["reference_capital"], "reference capital") == 0:
            if snapshot.get("account_kind") == "vault":
                changes["reference_capital"] = str(snapshot.get("vault", {}).get("vault_equity") or "0")
            else:
                changes["reference_capital"] = str(snapshot.get("account", {}).get("balance") or "0")
        if changes:
            policy_record = store.update_policy(user_name, changes, baseline_net_pnl=cumulative_pnl)
        else:
            policy_record = store.reset_baseline(user_name, cumulative_pnl)

    evaluation_pnl = cumulative_pnl
    if not commit and state.get("last_successful_scan_at") is None and policy_record["policy"]["baseline_mode"] == "from_enable":
        evaluation_pnl = state["baseline_pnl"]
    minimum_override = (
        policy_record["policy"]["vault_minimum_transfer_amount"]
        if snapshot.get("account_kind") == "vault"
        else None
    )
    decision = store.evaluate_dry(
        user_name,
        cumulative_net_pnl=evaluation_pnl,
        max_transferable=max_transferable,
        minimum_transfer_override=minimum_override,
        commit=commit,
    )
    if commit:
        now_s = int(time.time())
        latest = store.get_policy(user_name)
        configured = latest["policy"]
        if decision["would_transfer"]:
            cooldown_field = "vault_transfer_cooldown" if snapshot.get("account_kind") == "vault" else "successful_transfer_cooldown"
            base_delay = int(configured[cooldown_field])
        else:
            base_delay = int(configured["periodic_interval"])
        next_run = now_s + _jittered_delay(user_name, base_delay, configured["schedule_jitter_percent"])
        store.set_scheduler_hints(
            user_name,
            last_successful_scan_at=now_s,
            next_run_at=next_run,
        )
    return {
        "policy": store.get_policy(user_name),
        "decision": decision,
        "snapshot": snapshot,
        "read_only": True,
    }


def _require_live_snapshot(policy: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    """Reject incomplete, stale, or write-incapable preflight snapshots."""

    if not snapshot.get("complete"):
        messages = ", ".join(str(item.get("code") or "read_error") for item in snapshot.get("errors", []))
        raise RuntimeError(f"Exchange snapshot incomplete: {messages or 'unknown'}")
    try:
        collected_at_ms = int(snapshot["collected_at_ms"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("Exchange snapshot has no valid collection time") from exc
    age_ms = int(time.time() * 1000) - collected_at_ms
    if age_ms < 0 or age_ms > int(policy["maximum_preflight_age"]) * 1000:
        raise RuntimeError("Exchange snapshot is stale for Live submission")
    history = snapshot.get("history")
    if isinstance(history, dict) and history.get("fresh") is not True:
        raise RuntimeError("Exchange history is not fresh for Live submission")
    return snapshot


def _operation_id(exchange: str) -> str:
    """Allocate one random stable operation ID, canonical UUID for Bybit."""

    value = uuid.uuid4()
    return str(value) if exchange.lower() == "bybit" else value.hex


def _test_snapshot(user: Any, asset: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Collect and validate a fresh write-capable internal-route snapshot."""

    now_ms = int(time.time() * 1000)
    snapshot = collect_readonly_snapshot(user, max(0, now_ms - 300_000), now_ms, 30.0, asset)
    _require_live_snapshot(default_policy(), snapshot)
    capability = transfer_capability(user, snapshot)
    if not capability.get("supported") or not capability.get("writes_available"):
        raise ValueError(str(capability.get("reason") or "Manual test transfers are unavailable"))
    routes = capability.get("routes")
    test_route = "vault_to_main_perps" if snapshot.get("account_kind") == "vault" else routes[0] if isinstance(routes, list) and len(routes) == 1 else None
    if not isinstance(routes, list) or test_route not in routes:
        raise ValueError("A single fixed test-transfer route could not be resolved")
    return snapshot, {**capability, "test_route": test_route}


def _test_amount(value: Any) -> str:
    """Validate one positive exact manual-transfer amount."""

    if not isinstance(value, str) or value != value.strip() or not value:
        raise ValueError("amount must be a positive decimal string")
    amount = _decimal(value, "amount")
    if amount <= 0:
        raise ValueError("amount must be greater than zero")
    return _decimal_text(amount)


def _test_asset(snapshot: dict[str, Any]) -> str:
    """Read the server-snapshotted settlement asset for a manual transfer."""

    asset = snapshot.get("asset") if isinstance(snapshot.get("asset"), dict) else {}
    symbol = str(asset.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError("Fresh snapshot has no settlement asset")
    return symbol


def _requested_test_asset(user_name: str, user: Any, value: Any) -> str:
    """Resolve one explicit or persisted server-validated test settlement asset."""

    requested = str(value or "").strip().upper()
    if requested:
        return requested
    try:
        return str(_store().get_policy(user_name)["policy"]["asset"])
    except KeyError:
        if str(getattr(user, "exchange", "") or "").lower() == "hyperliquid":
            return "USDC"
        return str(getattr(user, "quote", "") or default_policy()["asset"]).strip().upper()


def _requested_test_operation_id(value: Any) -> str:
    """Require a canonical client idempotency UUID for manual real-funds requests."""

    if not isinstance(value, str) or value != value.strip():
        raise ValueError("operation_id must be a canonical UUID")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise ValueError("operation_id must be a canonical UUID") from exc
    canonical = str(parsed)
    if value.lower() != canonical:
        raise ValueError("operation_id must be a canonical UUID")
    return canonical


def _public_test_operation(operation: dict[str, Any], *, can_transfer_back: bool = False) -> dict[str, Any]:
    """Project a test operation without exposing descriptors, routes, or payloads."""

    error = operation["error"]
    submission = operation.get("submission")
    descriptor = operation.get("descriptor")
    vault_return_below_minimum = False
    if isinstance(descriptor, dict) and descriptor.get("adapter") == "hyperliquid_vault":
        returned_amount = operation.get("actual_amount") or operation.get("requested_amount") or "0"
        vault_return_below_minimum = Decimal(str(returned_amount)) < _VAULT_TEST_MINIMUM
    if (
        isinstance(submission, dict)
        and isinstance(submission.get("error"), dict)
        and submission["error"].get("type") == "AuthenticationError"
        and isinstance(descriptor, dict)
        and descriptor.get("adapter") == "binance_um"
    ):
        error = {
            "category": "exchange_permission",
            "reason": BINANCE_TRANSFER_PERMISSION_REASON,
        }
    if (
        isinstance(submission, dict)
        and isinstance(submission.get("error"), dict)
        and submission["error"].get("type") == "PermissionDenied"
        and isinstance(descriptor, dict)
        and str(descriptor.get("adapter") or "").startswith("bitget_")
    ):
        if descriptor.get("route") in {"usdt_futures_to_p2p", "futures_to_p2p"}:
            error = {
                "category": "obsolete_route",
                "reason": "This operation used the obsolete Bitget P2P route. PBGui now transfers between USDT Futures and Spot.",
            }
        else:
            error = {"category": "exchange_permission", "reason": BITGET_TRANSFER_PERMISSION_REASON}
    if (
        isinstance(submission, dict)
        and isinstance(submission.get("error"), dict)
        and submission["error"].get("type") == "InvalidOrder"
        and isinstance(descriptor, dict)
        and descriptor.get("adapter") == "hyperliquid_vault"
        and (not isinstance(error, dict) or not error.get("reason") or error.get("reason") == "failed")
    ):
        error = {
            "category": "exchange_rejected",
            "reason": (
                "Hyperliquid rejected vaultTransfer. This operation was recorded before PBGui retained the "
                "redacted provider reason; run a new test after the API update for exact guidance."
            ),
        }
    if (
        isinstance(submission, dict)
        and isinstance(submission.get("error"), dict)
        and submission["error"].get("type") == "Error"
        and isinstance(descriptor, dict)
        and descriptor.get("adapter") == "hyperliquid_vault"
        and (not isinstance(error, dict) or not error.get("reason"))
    ):
        error = {
            "category": "local_signing_error",
            "reason": (
                "PBGui rejected this operation locally before submission because the Vault signing context used "
                "the wrong address encoding. No request reached Hyperliquid and no funds moved."
            ),
        }
    return {
        "operation_id": operation["operation_id"],
        "parent_id": operation["parent_id"],
        "direction": operation["direction"],
        "status": operation["state"],
        "requested_amount": operation["requested_amount"],
        "actual_amount": operation["actual_amount"],
        "asset": str((operation.get("descriptor") or {}).get("asset") or ""),
        "prepared_at": operation["prepared_at"],
        "submitted_at": operation["submitted_at"],
        "resolved_at": operation["resolved_at"],
        "error": error,
        "can_transfer_back": bool(
            can_transfer_back and operation["state"] == "confirmed" and not vault_return_below_minimum
        ),
        "transfer_back_reason": (
            "Transfer back is unavailable because Hyperliquid Vault deposits require at least 5 USDC."
            if operation["state"] == "confirmed" and vault_return_below_minimum
            else None
        ),
    }


def _public_live_intent(
    intent: dict[str, Any],
    *,
    can_reconcile: bool = False,
) -> dict[str, Any]:
    """Project one Live intent without exposing fixed requests or account addresses."""

    error = intent.get("error") if isinstance(intent.get("error"), dict) else None
    submission = intent.get("submission") if isinstance(intent.get("submission"), dict) else {}
    reconciliation = (
        submission.get("reconciliation")
        if isinstance(submission.get("reconciliation"), dict)
        else None
    )
    public_reconciliation = None
    if reconciliation is not None:
        public_reconciliation = {
            "status": str(reconciliation.get("status") or "unknown")[:32],
            "reason": str(reconciliation.get("reason") or "")[:128] or None,
        }
    return {
        "operation_id": intent["operation_id"],
        "parent_id": intent["parent_id"],
        "leg": intent["leg"],
        "route": intent["route"],
        "state": intent["state"],
        "reserved_amount": intent["reserved_amount"],
        "prepared_at": intent["prepared_at"],
        "updated_at": intent["updated_at"],
        "submitted_at": intent["submitted_at"],
        "resolved_at": intent["resolved_at"],
        "error": error,
        "reconciliation": public_reconciliation,
        "can_reconcile": can_reconcile or intent["state"] == "unknown",
    }


def _execute_test_operation(
    user: Any,
    operation: dict[str, Any],
    *,
    browser_signature: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Submit and reconcile one newly prepared test operation exactly once."""

    store = _store()
    operation_id = operation["operation_id"]
    submitting = {
        "status": "submitting",
        "submitted_at_ms": int(time.time() * 1000),
    }
    operation = store.transition_test_operation(operation_id, submission=submitting, claim=True)
    submission = (
        submit_browser_signed_transfer(user, operation["descriptor"], browser_signature)
        if browser_signature is not None
        else submit_transfer(user, operation["descriptor"])
    )
    operation = store.transition_test_operation(operation_id, submission=submission)
    reconciliation: dict[str, Any] = {"status": "pending"}
    for attempt in range(_TEST_RECONCILE_ATTEMPTS):
        reconciliation = reconcile_transfer(user, operation["descriptor"], submission)
        if reconciliation.get("status") != "pending":
            break
        if attempt + 1 < _TEST_RECONCILE_ATTEMPTS:
            time.sleep(_TEST_RECONCILE_DELAY_SECONDS)
    persisted_submission = {**submission, "reconciliation": dict(reconciliation)}
    store.transition_test_operation(operation_id, submission=persisted_submission)
    return store.reconcile_test_operation(operation_id, reconciliation)


def _test_transfer_sync(
    user_name: str,
    amount_value: Any,
    asset_value: Any,
    operation_id_value: Any,
) -> dict[str, Any]:
    """Prepare, persist, submit, and reconcile one forward test transfer."""

    user = _user_or_404(user_name)
    amount = _test_amount(amount_value)
    asset = _requested_test_asset(user_name, user, asset_value)
    operation_id = _requested_test_operation_id(operation_id_value)
    try:
        existing = _store().get_test_operation(operation_id)
    except KeyError:
        existing = None
    if existing is not None:
        if existing["user_name"] != user_name or existing["direction"] != "forward":
            raise ValueError("operation_id is already assigned to another test transfer")
        existing_asset = str((existing.get("descriptor") or {}).get("asset") or "")
        if Decimal(existing["requested_amount"]) != Decimal(amount) or existing_asset != asset:
            raise ValueError("operation_id does not match the original test amount and asset")
        public_operation = _public_test_operation(
            existing,
            can_transfer_back=existing["state"] == "confirmed",
        )
        return {
            "status": existing["state"],
            "can_transfer_back": public_operation["can_transfer_back"],
            "operation": public_operation,
        }
    snapshot, capability = _test_snapshot(user, asset)
    if snapshot.get("account_kind") == "vault":
        vault = snapshot.get("vault") if isinstance(snapshot.get("vault"), dict) else {}
        positions = vault.get("positions") if isinstance(vault.get("positions"), list) else []
        orders = vault.get("orders") if isinstance(vault.get("orders"), list) else []
        if bool(vault.get("always_close_on_withdraw")) and (positions or orders):
            raise ValueError(
                "Vault test withdrawal blocked: Hyperliquid reports alwaysCloseOnWithdraw and the Vault currently "
                f"has {len(positions)} open position(s) and {len(orders)} open order(s). Stop trading, flatten the "
                "Vault, and cancel its orders before testing so the withdrawal cannot alter active trading."
            )
        try:
            vault_policy = _store().get_policy(user_name)["policy"]
        except KeyError:
            vault_policy = default_policy()
        test_policy = {**vault_policy, "vault_withdraw_mode": "margin_buffered"}
        transferable = _decimal(_vault_transferable(test_policy, snapshot), "transferable amount")
    else:
        account = snapshot.get("account") if isinstance(snapshot.get("account"), dict) else {}
        transferable = _decimal(account.get("withdrawable"), "transferable amount")
    if transferable < 0:
        raise ValueError("Fresh snapshot returned a negative transferable amount")
    if snapshot.get("account_kind") == "vault" and transferable == 0:
        raise ValueError("Vault safety constraints currently allow no test withdrawal")
    if Decimal(amount) > transferable:
        raise ValueError("Test transfer amount exceeds the fresh transferable amount")
    route = str(capability["test_route"])
    descriptor = prepare_transfer(
        user,
        operation_id=operation_id,
        amount=amount,
        asset=_test_asset(snapshot),
        route=route,
        snapshot=snapshot,
        nonce=int(time.time() * 1000) if str(getattr(user, "exchange", "")).lower() == "hyperliquid" else None,
    )
    operation = _store().create_test_operation(
        user_name,
        operation_id=operation_id,
        parent_id=None,
        direction="forward",
        route=route,
        descriptor=descriptor,
        requested_amount=descriptor["amount"],
    )
    resolved = _execute_test_operation(user, operation)
    public_operation = _public_test_operation(resolved, can_transfer_back=resolved["state"] == "confirmed")
    return {
        "status": resolved["state"],
        "can_transfer_back": public_operation["can_transfer_back"],
        "operation": public_operation,
    }


def _test_transfer_back_sync(user_name: str, operation_id: str) -> dict[str, Any]:
    """Reverse one confirmed forward test transfer exactly once."""

    user = _user_or_404(user_name)
    forward = _store().get_test_operation(operation_id)
    if forward["user_name"] != user_name or forward["direction"] != "forward":
        raise KeyError("Profit Sweep test operation not found")
    if forward["state"] != "confirmed":
        raise ValueError("Only a confirmed forward test transfer can be sent back")
    if any(
        item["direction"] == "back" and item.get("parent_id") == operation_id
        for item in _store().list_test_operations(user_name)
    ):
        raise ValueError("Test transfer has already been sent back")
    amount = str(forward.get("actual_amount") or forward["requested_amount"])
    if bool(getattr(user, "is_vault", False)) and Decimal(amount) < _VAULT_TEST_MINIMUM:
        raise ValueError("Transfer back requires at least 5 USDC because Hyperliquid enforces that Vault deposit minimum")
    snapshot, _capability = _test_snapshot(user, str(forward["descriptor"]["asset"]))
    reverse_route = reverse_transfer_route(user, snapshot, forward["route"])
    if snapshot.get("account_kind") == "vault":
        balances = snapshot.get("account_balances") if isinstance(snapshot.get("account_balances"), dict) else {}
        destinations = balances.get("destination") if isinstance(balances.get("destination"), dict) else {}
        main_perps = destinations.get("main_perps") if isinstance(destinations.get("main_perps"), dict) else {}
        main_balance = _decimal(main_perps.get("balance"), "Main Perps balance")
        if Decimal(amount) > main_balance:
            raise ValueError("Test transfer return amount exceeds the fresh Main Perps balance")
    reverse_id = _operation_id(str(getattr(user, "exchange", "") or ""))
    previous_nonce = forward["descriptor"].get("request", {}).get("nonce")
    nonce = None
    if str(getattr(user, "exchange", "")).lower() == "hyperliquid":
        nonce = max(int(time.time() * 1000), int(previous_nonce or 0) + 1)
    descriptor = prepare_transfer(
        user,
        operation_id=reverse_id,
        amount=amount,
        asset=forward["descriptor"]["asset"],
        route=reverse_route,
        snapshot=snapshot,
        nonce=nonce,
    )
    operation = _store().create_test_operation(
        user_name,
        operation_id=reverse_id,
        parent_id=operation_id,
        direction="back",
        route=reverse_route,
        descriptor=descriptor,
        requested_amount=descriptor["amount"],
    )
    resolved = _execute_test_operation(user, operation)
    return {
        "status": resolved["state"],
        "can_transfer_back": False,
        "operation": _public_test_operation(resolved),
    }


def _submit_test_signature_sync(user_name: str, operation_id: str, signature: str) -> dict[str, Any]:
    """Verify and submit one prepared Vault test operation exactly once."""

    user = _user_or_404(user_name)
    operation = _store().get_test_operation(operation_id)
    if operation["user_name"] != user_name or operation["state"] != "prepared":
        raise ValueError("Vault test operation is not awaiting a wallet signature")
    descriptor = operation.get("descriptor") if isinstance(operation.get("descriptor"), dict) else {}
    if descriptor.get("adapter") != "hyperliquid_vault":
        raise ValueError("Wallet signatures are accepted only for Hyperliquid Vault test operations")
    prepared_at = int(descriptor.get("prepared_at_ms") or 0)
    if prepared_at <= 0 or int(time.time() * 1000) - prepared_at > 300_000:
        raise ValueError("Vault test signature request expired; start a new test transfer")
    snapshot, _capability = _test_snapshot(user, str(descriptor["asset"]))
    leader = snapshot.get("leader") if isinstance(snapshot.get("leader"), dict) else {}
    leader_address = str(leader.get("address") or "")
    vault = snapshot.get("vault") if isinstance(snapshot.get("vault"), dict) else {}
    if operation["direction"] == "forward":
        if descriptor.get("route") != "vault_to_main_perps":
            raise ValueError("Prepared Vault test route is invalid")
        positions = vault.get("positions") if isinstance(vault.get("positions"), list) else []
        orders = vault.get("orders") if isinstance(vault.get("orders"), list) else []
        if bool(vault.get("always_close_on_withdraw")) and (positions or orders):
            raise ValueError("Vault trading activity changed; flatten the Vault and start a new test transfer")
        try:
            vault_policy = _store().get_policy(user_name)["policy"]
        except KeyError:
            vault_policy = default_policy()
        test_policy = {**vault_policy, "vault_withdraw_mode": "margin_buffered"}
        transferable = _decimal(_vault_transferable(test_policy, snapshot), "transferable amount")
        if Decimal(descriptor["amount"]) > transferable:
            raise ValueError("Prepared Vault test amount exceeds the refreshed transferable cap")
        if descriptor.get("destination") != leader_address:
            raise ValueError("Vault Leader changed; start a new test transfer")
    else:
        if descriptor.get("route") != "main_perps_to_vault" or descriptor.get("source") != leader_address:
            raise ValueError("Prepared Vault return route no longer matches the Leader")
        if Decimal(descriptor["amount"]) < _VAULT_TEST_MINIMUM:
            raise ValueError("Transfer back requires at least 5 USDC")
        balances = snapshot.get("account_balances") if isinstance(snapshot.get("account_balances"), dict) else {}
        destinations = balances.get("destination") if isinstance(balances.get("destination"), dict) else {}
        main_perps = destinations.get("main_perps") if isinstance(destinations.get("main_perps"), dict) else {}
        if Decimal(descriptor["amount"]) > _decimal(main_perps.get("balance"), "Main balance"):
            raise ValueError("Prepared return amount exceeds the refreshed Leader Main balance")
    try:
        verified = verify_browser_signature(user, descriptor, signature, leader_address)
    except ValueError:
        _store().transition_test_operation(
            operation_id,
            submission={
                "status": "failed",
                "error": {"category": "wallet_signature", "type": "SignatureRejected"},
            },
        )
        _store().reconcile_test_operation(
            operation_id,
            {"status": "failed", "reason": "wallet_signature_rejected"},
        )
        raise
    resolved = _execute_test_operation(user, operation, browser_signature=verified)
    public_operation = _public_test_operation(
        resolved,
        can_transfer_back=resolved["state"] == "confirmed" and operation["direction"] == "forward",
    )
    return {
        "status": resolved["state"],
        "can_transfer_back": public_operation["can_transfer_back"],
        "operation": public_operation,
    }


def _intent_route(user: Any, snapshot: dict[str, Any], policy: dict[str, Any]) -> str:
    """Select one server-owned fixed internal route from adapter capability."""

    exchange = str(getattr(user, "exchange", "") or "").lower()
    asset = str(policy.get("asset") or "")
    allowed_assets = {
        "hyperliquid": {"USDC"},
        "bybit": {"USDT", "USDC"},
        "binance": {"USDT", "USDC"},
        "bitget": {"USDT"},
    }
    if asset not in allowed_assets.get(exchange, set()):
        raise ValueError(f"{asset or 'Configured asset'} is not supported by the {exchange} transfer adapter")
    capability = transfer_capability(user, snapshot)
    if not capability.get("supported") or not capability.get("writes_available"):
        raise ValueError(str(capability.get("reason") or "Profit Sweep writes are unavailable"))
    if exchange == "hyperliquid" and snapshot.get("account_kind") == "vault":
        return "vault_to_main_perps"
    routes = capability.get("routes")
    if not isinstance(routes, list) or not routes:
        raise ValueError("No internal Profit Sweep route is available")
    return str(routes[0])


def _prepare_persisted_intent(
    user: Any,
    policy_record: dict[str, Any],
    snapshot: dict[str, Any],
    *,
    amount: str,
    operation_id: str,
    parent_id: str | None,
    leg: int,
    route: str,
    nonce: int | None = None,
    reservation_guard: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build through the sealed adapter and persist before external I/O."""

    descriptor = prepare_transfer(
        user,
        operation_id=operation_id,
        amount=amount,
        asset=policy_record["policy"]["asset"],
        route=route,
        snapshot=snapshot,
        nonce=nonce,
    )
    return _store().create_live_intent(
        policy_record["user_name"],
        operation_id=operation_id,
        parent_id=parent_id,
        leg=leg,
        route=route,
        descriptor=descriptor,
        reserved_amount=descriptor["amount"],
        reservation_guard=reservation_guard,
    )


def _execute_prepared_intent(
    user: Any,
    intent: dict[str, Any],
    *,
    settle_financial: bool,
    require_received: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Submit a prepared intent once, persist its result, then reconcile it."""

    operation_id = intent["operation_id"]
    submitted_at_ms = int(time.time() * 1000)
    intent = _store().transition_live_intent(
        operation_id,
        "submitting",
        submission={"status": "submitting", "submitted_at_ms": submitted_at_ms},
        claim=True,
    )
    submission = submit_transfer(user, intent["descriptor"])
    intent = _store().transition_live_intent(operation_id, "submitting", submission=submission)
    reconciliation = reconcile_transfer(user, intent["descriptor"], submission)
    if require_received and reconciliation.get("status") == "confirmed" and not reconciliation.get("received_amount"):
        reconciliation = {
            **reconciliation,
            "status": "unknown",
            "reason": "received_amount_missing",
        }
    persisted_submission = {**submission, "reconciliation": dict(reconciliation)}
    _store().transition_live_intent(operation_id, "submitting", submission=persisted_submission)
    accounting_amount = reconciliation.get("received_amount") if settle_financial else None
    resolved = _store().reconcile_live_intent(
        operation_id,
        reconciliation,
        settle_financial=settle_financial,
        accounting_amount=accounting_amount,
    )
    return resolved, reconciliation


def _reconcile_existing_intent(user: Any, intent: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Reconcile a previously submitted intent without ever submitting it again."""

    submission = intent.get("submission") or {
        "status": "unknown",
        "submitted_at_ms": int(intent["submitted_at"] or intent["updated_at"]) * 1000,
    }
    reconciliation = reconcile_transfer(user, intent["descriptor"], submission)
    is_vault_leg_one = intent["route"] == "vault_to_main_perps" and intent.get("parent_id") is not None
    forwards_to_spot = is_vault_leg_one and intent["operation_id"] != intent["parent_id"]
    requires_received = is_vault_leg_one
    if requires_received and reconciliation.get("status") == "confirmed" and not reconciliation.get("received_amount"):
        reconciliation = {
            **reconciliation,
            "status": "unknown",
            "reason": "received_amount_missing",
        }
    persisted_submission = {**submission, "reconciliation": dict(reconciliation)}
    _store().transition_live_intent(
        intent["operation_id"], intent["state"], submission=persisted_submission
    )
    resolved = _store().reconcile_live_intent(
        intent["operation_id"],
        reconciliation,
        settle_financial=not forwards_to_spot,
        accounting_amount=reconciliation.get("received_amount") if not forwards_to_spot else None,
    )
    return resolved, reconciliation


def _create_vault_leg_two_unchecked(
    user: Any,
    parent_intent: dict[str, Any],
    received_amount: str,
    *,
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create and execute the optional Vault main-perps-to-spot leg."""

    store = _store()
    policy_record = store.get_policy(parent_intent["user_name"])
    existing = [
        item
        for item in store.list_live_intents(parent_intent["user_name"])
        if item.get("parent_id") == parent_intent["parent_id"] and item["leg"] == 2
    ]
    active_existing = [item for item in existing if item["state"] != "failed"]
    if active_existing:
        return active_existing[-1]
    if snapshot is None:
        now_ms = int(time.time() * 1000)
        since_ms, until_ms = _history_window(policy_record, now_ms, state_kind="live")
        snapshot = collect_readonly_snapshot(
            user, since_ms, until_ms, 30.0, policy_record["policy"]["asset"]
        )
    _require_live_snapshot(policy_record["policy"], snapshot)
    capability = transfer_capability(user, snapshot)
    if "main_perps_to_spot" not in capability.get("routes", []):
        raise ValueError("Vault main-perps-to-spot capability is unavailable")
    operation_id = (
        f"{parent_intent['parent_id']}-leg2"
        if not existing
        else f"{parent_intent['parent_id']}-leg2-retry-{len(existing)}"
    )
    nonce_source = existing[-1] if existing else parent_intent
    previous_nonce = int(nonce_source["descriptor"]["request"]["nonce"])
    intent = _prepare_persisted_intent(
        user,
        policy_record,
        snapshot,
        amount=received_amount,
        operation_id=operation_id,
        parent_id=parent_intent["parent_id"],
        leg=2,
        route="main_perps_to_spot",
        nonce=max(int(time.time() * 1000), previous_nonce + 1),
    )
    resolved, _reconciliation = _execute_prepared_intent(user, intent, settle_financial=True)
    return resolved


def _create_vault_leg_two(
    user: Any,
    parent_intent: dict[str, Any],
    received_amount: str,
    *,
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create Vault forwarding or durably pause until that leg can be recovered."""

    try:
        return _create_vault_leg_two_unchecked(
            user,
            parent_intent,
            received_amount,
            snapshot=snapshot,
        )
    except Exception:
        store = _store()
        if not store.list_live_intents(parent_intent["user_name"], unresolved_only=True):
            try:
                store.update_policy(
                    parent_intent["user_name"],
                    {"operating_mode": "paused_unknown"},
                )
            except Exception:
                pass
        raise


def _evaluate_live_sync(user_name: str) -> dict[str, Any]:
    """Evaluate, reserve, submit once, and reconcile one Live policy."""

    store = _store()
    user = _user_or_404(user_name)
    policy_record = store.get_policy(user_name)
    if policy_record["exchange"] != str(user.exchange):
        raise ValueError("Policy exchange no longer matches the exchange user")
    if policy_record["policy"]["operating_mode"] != "live":
        raise ValueError("Live evaluation requires operating_mode=live")
    if store.list_live_intents(user_name, unresolved_only=True):
        raise ValueError("An unresolved Live intent blocks new work")
    now_ms = int(time.time() * 1000)
    since_ms, until_ms = _history_window(policy_record, now_ms, state_kind="live")
    snapshot = collect_readonly_snapshot(
        user, since_ms, until_ms, 30.0, policy_record["policy"]["asset"]
    )
    _require_live_snapshot(policy_record["policy"], snapshot)
    _intent_route(user, snapshot, policy_record["policy"])
    cumulative_pnl, max_transferable = _snapshot_pnl_and_cap(store, policy_record, snapshot)
    minimum_override = (
        policy_record["policy"]["vault_minimum_transfer_amount"]
        if snapshot.get("account_kind") == "vault"
        else None
    )
    decision = store.evaluate_live(
        user_name,
        cumulative_net_pnl=cumulative_pnl,
        max_transferable=max_transferable,
        minimum_transfer_override=minimum_override,
    )
    result: dict[str, Any] = {"policy": store.get_policy(user_name), "decision": decision, "snapshot": snapshot}
    if not decision["would_transfer"]:
        now_s = int(time.time())
        store.set_scheduler_hints(
            user_name,
            state_kind="live",
            last_successful_scan_at=now_s,
            next_run_at=now_s
            + _jittered_delay(
                user_name,
                int(policy_record["policy"]["periodic_interval"]),
                policy_record["policy"]["schedule_jitter_percent"],
            ),
        )
        return result

    route = _intent_route(user, snapshot, policy_record["policy"])
    root_id = _operation_id(policy_record["exchange"])
    vault_to_spot = snapshot.get("account_kind") == "vault" and policy_record["policy"]["vault_destination"] == "main_spot"
    is_vault = snapshot.get("account_kind") == "vault"
    operation_id = f"{root_id}-leg1" if vault_to_spot else root_id
    intent = _prepare_persisted_intent(
        user,
        policy_record,
        snapshot,
        amount=decision["amount"],
        operation_id=operation_id,
        parent_id=root_id if is_vault else None,
        leg=1,
        route=route,
        nonce=int(time.time() * 1000) if policy_record["exchange"].lower() == "hyperliquid" else None,
        reservation_guard=decision["reservation_guard"],
    )
    result["decision"]["reserved_total"] = intent["reserved_amount"]
    resolved, reconciliation = _execute_prepared_intent(
        user,
        intent,
        settle_financial=not vault_to_spot,
        require_received=vault_to_spot,
    )
    result["intent"] = resolved
    if vault_to_spot and resolved["state"] == "confirmed":
        result["intent_leg2"] = _create_vault_leg_two(
            user, resolved, str(reconciliation["received_amount"]), snapshot=snapshot
        )
    now_s = int(time.time())
    delay_field = "vault_transfer_cooldown" if snapshot.get("account_kind") == "vault" else "successful_transfer_cooldown"
    store.set_scheduler_hints(
        user_name,
        state_kind="live",
        last_successful_scan_at=now_s,
        next_run_at=now_s
        + _jittered_delay(
            user_name,
            int(policy_record["policy"][delay_field]),
            policy_record["policy"]["schedule_jitter_percent"],
        ),
    )
    return {**result, "policy": store.get_policy(user_name)}


def _preview_sync(user_name: str, policy_overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Evaluate current form values without requiring or mutating a saved policy."""

    store = _store()
    user = _user_or_404(user_name)
    try:
        saved = store.get_policy(user_name)
    except KeyError:
        saved = None
    policy = {**(saved["policy"] if saved is not None else default_policy()), **(policy_overrides or {})}
    now_ms = int(time.time() * 1000)
    if saved is not None:
        since_ms, until_ms = _history_window(saved, now_ms)
    else:
        lookback = int(policy.get("maximum_history_age") or 86_400)
        since_ms, until_ms = max(0, now_ms - lookback * 1000), now_ms
    snapshot = collect_readonly_snapshot(user, since_ms, until_ms, 30.0, policy["asset"])
    if not snapshot.get("complete"):
        messages = ", ".join(str(item.get("code") or "read_error") for item in snapshot.get("errors", []))
        raise RuntimeError(f"Exchange snapshot incomplete: {messages or 'unknown'}")
    asset_changed = saved is not None and policy["asset"] != saved["policy"]["asset"]
    if saved is None or asset_changed:
        cumulative_pnl, max_transferable = _transient_snapshot_pnl_and_cap(policy, snapshot)
        baseline = cumulative_pnl if policy.get("baseline_mode") == "from_enable" else "0"
        high_watermark = "0"
        sweep_due = "0"
        generation = 0 if saved is None else int(saved["generation"]) + 1
    else:
        cumulative_pnl, max_transferable = _snapshot_pnl_and_cap(
            store, saved, snapshot, cap_policy=policy
        )
        state = saved["simulation_state"]
        baseline = state["baseline_pnl"]
        if state.get("last_successful_scan_at") is None and policy.get("baseline_mode") == "from_enable":
            baseline = cumulative_pnl
        high_watermark = state["high_watermark"]
        sweep_due = state["sweep_due"]
        generation = state["generation"]
    if _decimal(policy.get("reference_capital") or "0", "reference capital") == 0:
        if snapshot.get("account_kind") == "vault":
            policy["reference_capital"] = str(snapshot.get("vault", {}).get("vault_equity") or "0")
        else:
            policy["reference_capital"] = str(snapshot.get("account", {}).get("balance") or "0")
    net_pnl = _decimal(cumulative_pnl, "cumulative PnL") - _decimal(baseline, "baseline PnL")
    minimum_override = policy.get("vault_minimum_transfer_amount") if snapshot.get("account_kind") == "vault" else None
    decision = calculate_sweep(
        policy,
        net_pnl=_decimal_text(net_pnl),
        high_watermark=high_watermark,
        sweep_due=sweep_due,
        max_transferable=max_transferable,
        state_kind="simulation",
        minimum_transfer_override=minimum_override,
    )
    return {
        "policy": {
            "user_name": user_name,
            "exchange": str(user.exchange),
            "generation": generation,
            "policy": policy,
        },
        "decision": {**decision, "committed": False},
        "snapshot": snapshot,
        "read_only": True,
        "saved_policy": saved is not None,
    }


def _activate_live_sync(user_name: str, expected_policy_fingerprint: str) -> dict[str, Any]:
    """Run server-owned Live capability, freshness, and baseline activation gates."""

    store = _store()
    user = _user_or_404(user_name)
    policy_record = store.get_policy(user_name)
    if _policy_fingerprint(policy_record["policy"]) != expected_policy_fingerprint:
        raise ValueError("Policy changed after Live confirmation")
    if policy_record["exchange"] != str(user.exchange):
        raise ValueError("Policy exchange no longer matches the exchange user")
    if policy_record["policy"]["operating_mode"] not in {"disabled", "dry"}:
        raise ValueError("Live activation requires a disabled or dry policy")
    now_ms = int(time.time() * 1000)
    since_ms, until_ms = _history_window(policy_record, now_ms)
    snapshot = collect_readonly_snapshot(
        user, since_ms, until_ms, 30.0, policy_record["policy"]["asset"]
    )
    _require_live_snapshot(policy_record["policy"], snapshot)
    capability = transfer_capability(user, snapshot)
    if not capability.get("supported") or not capability.get("writes_available"):
        raise ValueError(str(capability.get("reason") or "Profit Sweep writes are unavailable"))
    _intent_route(user, snapshot, policy_record["policy"])
    cumulative_pnl, _max_transferable = _snapshot_pnl_and_cap(store, policy_record, snapshot)
    activated = store.activate_live(
        user_name,
        cumulative_pnl,
        baseline_mode=policy_record["policy"]["live_activation_baseline_mode"],
        expected_policy_fingerprint=expected_policy_fingerprint,
    )
    now_s = int(time.time())
    delay = max(
        int(activated["policy"]["settlement_debounce"]),
        int(activated["policy"]["quiet_period"]),
    ) + int(activated["policy"]["stabilization_interval"])
    store.set_scheduler_hints(
        user_name,
        state_kind="live",
        last_successful_scan_at=now_s,
        next_run_at=now_s + delay,
    )
    return {
        "policy": _public_policy_record(store.get_policy(user_name)),
        "capability": capability,
        "baseline_mode": activated["policy"]["live_activation_baseline_mode"],
        "snapshot": snapshot,
    }


async def _evaluate_user(user_name: str, *, commit: bool, stabilize: bool = False) -> dict[str, Any]:
    """Serialize one user's thread-backed evaluation and optionally stabilize it."""

    lock = _EVALUATION_LOCKS.setdefault(user_name, asyncio.Lock())
    async with lock:
        preview = await _run_owned_thread(_evaluate_sync, user_name, commit=False)
        if not stabilize:
            return preview
        event_before = preview["policy"]["simulation_state"].get("last_event_at")
        interval = int(preview["policy"]["policy"]["stabilization_interval"])
        if interval:
            await asyncio.sleep(interval)
        current = await _run_owned_thread(_store().get_policy, user_name)
        next_run = current["simulation_state"].get("next_run_at")
        event_after = current["simulation_state"].get("last_event_at")
        if event_after != event_before or (next_run is not None and int(next_run) > int(time.time())):
            preview["decision"] = {
                **preview["decision"],
                "amount": "0",
                "would_transfer": False,
                "reason": "stabilization_restarted",
            }
            preview["postponed"] = True
            return preview
        return await _run_owned_thread(_evaluate_sync, user_name, commit=commit)


async def _evaluate_live_user(user_name: str, *, stabilize: bool = False) -> dict[str, Any]:
    """Serialize one user's Live preflight and blocking transfer lifecycle."""

    lock = _EVALUATION_LOCKS.setdefault(user_name, asyncio.Lock())
    async with lock:
        if stabilize:
            record = await _run_owned_thread(_store().get_policy, user_name)
            interval = int(record["policy"]["stabilization_interval"])
            if interval:
                await asyncio.sleep(interval)
        return await _run_owned_thread(_evaluate_live_sync, user_name)


def _reconcile_operation_sync(user_name: str, operation_id: str) -> dict[str, Any]:
    """Reconcile one server-owned operation without accepting request metadata."""

    user = _user_or_404(user_name)
    intent = _store().get_live_intent(operation_id)
    if intent["user_name"] != user_name:
        raise KeyError("Profit Sweep intent not found")
    if intent["state"] == "prepared":
        is_vault_leg_one = intent["route"] == "vault_to_main_perps" and intent.get("parent_id") is not None
        forwards_to_spot = is_vault_leg_one and intent["operation_id"] != intent["parent_id"]
        resolved, reconciliation = _execute_prepared_intent(
            user,
            intent,
            settle_financial=not forwards_to_spot,
            require_received=is_vault_leg_one,
        )
    elif intent["state"] in {"submitting", "unknown"}:
        resolved, reconciliation = _reconcile_existing_intent(user, intent)
    else:
        resolved = intent
        submission = intent.get("submission") or {}
        reconciliation = (
            submission.get("reconciliation")
            if isinstance(submission.get("reconciliation"), dict)
            else {}
        )
    if (
        resolved["state"] == "confirmed"
        and resolved["route"] == "vault_to_main_perps"
        and resolved.get("parent_id") is not None
        and resolved["operation_id"] != resolved["parent_id"]
    ):
        received = reconciliation.get("received_amount")
        if received:
            return _create_vault_leg_two(user, resolved, str(received))
    return resolved


def _reconcile_test_operation_sync(user_name: str, operation_id: str) -> dict[str, Any]:
    """Reconcile one submitted test operation without ever resubmitting it."""

    operation = _store().get_test_operation(operation_id)
    if operation["user_name"] != user_name:
        raise KeyError("Profit Sweep test operation not found")
    if operation["state"] in {"confirmed", "failed", "prepared"}:
        return operation
    user = _user_or_404(user_name)
    submission = operation.get("submission") or {
        "status": "unknown",
        "submitted_at_ms": int(operation["submitted_at"] or operation["updated_at"]) * 1000,
    }
    reconciliation = reconcile_transfer(user, operation["descriptor"], submission)
    return _store().reconcile_test_operation(operation_id, reconciliation)


def _reconcile_unresolved_sync() -> None:
    """Reconcile every submitted durable operation before scheduler evaluation."""

    store = _store()
    for operation in store.list_unresolved_test_operations():
        if operation["state"] in {"submitting", "unknown"}:
            _reconcile_test_operation_sync(operation["user_name"], operation["operation_id"])
    for policy_record in store.list_policies():
        user_name = policy_record["user_name"]
        for intent in store.list_live_intents(user_name, unresolved_only=True):
            _reconcile_operation_sync(user_name, intent["operation_id"])
        intents = store.list_live_intents(user_name)
        leg_two_parents = {
            item.get("parent_id")
            for item in intents
            if item["leg"] == 2 and item["state"] != "failed"
        }
        for intent in intents:
            if (
                intent["state"] == "confirmed"
                and intent["leg"] == 1
                and intent["route"] == "vault_to_main_perps"
                and intent.get("parent_id") is not None
                and intent["operation_id"] != intent["parent_id"]
                and intent["parent_id"] not in leg_two_parents
            ):
                submission = intent.get("submission") or {}
                reconciliation = submission.get("reconciliation") if isinstance(submission, dict) else None
                received = reconciliation.get("received_amount") if isinstance(reconciliation, dict) else None
                if not received:
                    raise RuntimeError(f"Vault intent {intent['operation_id']} has no reconciled received amount")
                _create_vault_leg_two(_user_or_404(user_name), intent, str(received))


def _record_income_hint(user_name: str) -> bool:
    """Persist one non-authoritative PBData hint outside the event loop."""
    if not user_name:
        return False
    try:
        record = _store().get_policy(user_name)
        mode = record["policy"]["operating_mode"]
        if mode not in {"dry", "live"}:
            return False
        now = int(time.time())
        delay = max(int(record["policy"]["settlement_debounce"]), int(record["policy"]["quiet_period"]))
        next_run = now + delay
        _store().set_scheduler_hints(
            user_name,
            state_kind="simulation" if mode == "dry" else "live",
            last_event_at=now,
            next_run_at=next_run,
        )
    except (KeyError, ValueError):
        return False
    return True


async def notify_income(user_name: str) -> None:
    """Record a PBData income hint without blocking the API event loop."""

    updated = await asyncio.to_thread(_record_income_hint, user_name)
    wake = _SCHEDULER_WAKE
    if updated and wake is not None:
        wake.set()


async def _scheduler_loop() -> None:
    """Recover durable intents, then run bounded Dry and Live evaluations."""

    global _SCHEDULER_WAKE
    if _SCHEDULER_WAKE is None:
        _SCHEDULER_WAKE = asyncio.Event()
    try:
        await _run_owned_thread(_reconcile_unresolved_sync)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _log(
            SERVICE,
            f"Startup intent reconciliation failed: {type(exc).__name__}",
            level="ERROR",
            meta={"operation": "intent_recovery", "traceback": traceback.format_exc()},
        )
    while not _STOPPING:
        wait_seconds = 30
        try:
            now = int(time.time())
            due: list[tuple[str, str]] = []
            for record in await _run_owned_thread(_store().list_policies):
                mode = record["policy"]["operating_mode"]
                if mode not in {"dry", "live"}:
                    continue
                state_kind = "simulation" if mode == "dry" else "live"
                next_run = record[f"{state_kind}_state"].get("next_run_at")
                if next_run is None or int(next_run) <= now:
                    due.append((record["user_name"], mode))
                else:
                    wait_seconds = min(wait_seconds, max(1, int(next_run) - now))
            if due:
                user_name, mode = sorted(due, key=lambda item: item[0].lower())[0]
                try:
                    result = (
                        await _evaluate_user(user_name, commit=True, stabilize=True)
                        if mode == "dry"
                        else await _evaluate_live_user(user_name, stabilize=True)
                    )
                    decision = result["decision"]
                    _log(
                        SERVICE,
                        f"{mode.title()} evaluation for {user_name}: {decision['reason']} amount={decision['amount']}",
                        level="INFO",
                        user=user_name,
                        meta={"operation": f"{mode}_evaluate", "exchange": result["policy"]["exchange"]},
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _log(
                        SERVICE,
                        f"{mode.title()} evaluation failed for {user_name}: {type(exc).__name__}",
                        level="WARNING",
                        user=user_name,
                        meta={"operation": f"{mode}_evaluate", "traceback": traceback.format_exc()},
                    )
                    try:
                        record = _store().get_policy(user_name)
                        _store().set_scheduler_hints(
                            user_name,
                            state_kind="simulation" if mode == "dry" else "live",
                            next_run_at=int(time.time()) + min(300, int(record["policy"]["periodic_interval"])),
                        )
                    except Exception:
                        pass
                continue
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log(
                SERVICE,
                f"Scheduler tick failed: {type(exc).__name__}",
                level="ERROR",
                meta={"operation": "scheduler", "traceback": traceback.format_exc()},
            )
        wake = _SCHEDULER_WAKE
        if wake is None:
            await asyncio.sleep(wait_seconds)
            continue
        try:
            await asyncio.wait_for(wake.wait(), timeout=wait_seconds)
        except asyncio.TimeoutError:
            pass
        wake.clear()


def startup() -> None:
    """Initialize the private store and start the single API-owned scheduler."""

    global _SCHEDULER_TASK, _SCHEDULER_WAKE, _STOPPING
    _store()
    _STOPPING = False
    if _SCHEDULER_WAKE is None:
        _SCHEDULER_WAKE = asyncio.Event()
    if _SCHEDULER_TASK is None or _SCHEDULER_TASK.done():
        _SCHEDULER_TASK = asyncio.create_task(_scheduler_loop(), name="profit-sweep-scheduler")


async def shutdown() -> None:
    """Cancel the scheduler and await every API-owned Profit Sweep worker."""

    global _SCHEDULER_TASK, _SCHEDULER_WAKE, _STOPPING
    _STOPPING = True
    if _SCHEDULER_WAKE is not None:
        _SCHEDULER_WAKE.set()
    task = _SCHEDULER_TASK
    if task is not None and not task.done():
        task.cancel()
    if task is not None:
        await asyncio.gather(task, return_exceptions=True)
    active = list(_ACTIVE_OPERATION_TASKS)
    if active:
        await asyncio.gather(*active, return_exceptions=True)
    _SCHEDULER_TASK = None
    _SCHEDULER_WAKE = None
    _EVALUATION_LOCKS.clear()


def restart_block_reason() -> str:
    """Block API restart while an external submission may be in progress."""

    for policy in _store().list_policies():
        for intent in _store().list_live_intents(policy["user_name"], unresolved_only=True):
            if intent["state"] == "submitting":
                return (
                    f"Profit Sweep transfer {intent['operation_id']} for {policy['user_name']} "
                    "is submitting and must be reconciled before restart"
                )
    for operation in _store().list_unresolved_test_operations():
        if operation["state"] == "submitting":
            return (
                f"Profit Sweep test transfer {operation['operation_id']} for {operation['user_name']} "
                "is submitting and must be reconciled before restart"
            )
    return ""


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(request: Request, session: SessionToken = Depends(require_auth)) -> HTMLResponse:
    """Serve the standalone Profit Sweep page with cookie authentication."""

    html_path = Path(__file__).parent.parent / "frontend" / "profit_sweep.html"
    html = html_path.read_text(encoding="utf-8")
    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    html = html.replace('"%%API_BASE%%"', json.dumps(origin + "/api/profit-sweep"))
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)
    return HTMLResponse(
        content=html,
        headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"},
    )


@router.get("/schema")
def get_schema(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return current defaults and mode options without credential data."""

    return {
        "feature_status": "live",
        "live_available": True,
        "defaults": default_policy(),
        "options": {
            "operating_mode": ["disabled", "dry", "live", "paused_unknown"],
            "baseline_mode": ["from_enable", "lifetime"],
            "trigger_mode": ["hybrid", "interval"],
            "safety_reserve_mode": ["fixed", "percent", "max_of_both"],
            "vault_withdraw_mode": ["flat_only", "margin_buffered"],
            "vault_destination": ["main_perps", "main_spot"],
            "main_destination_activity_policy": ["warn", "pause_future_sweeps"],
        },
    }


@router.get("/users")
def list_users(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """List exchange users with secret-free Profit Sweep status."""

    policies = {item["user_name"]: item for item in _store().list_policies()}
    users = [_safe_user(user, policies.get(user.name)) for user in _users()]
    return {"users": sorted(users, key=lambda item: item["name"].lower())}


@router.get("/policies")
def list_policies(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return all persisted Profit Sweep policies."""

    return {"policies": [_public_policy_record(item) for item in _store().list_policies()]}


@router.get("/policies/{user_name}")
def get_policy(user_name: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return one policy and current Dry state."""

    _user_or_404(user_name)
    try:
        return _public_policy_record(_store().get_policy(user_name))
    except (KeyError, ValueError) as exc:
        raise _logged_http_error(
            404,
            "Profit Sweep policy not found",
            operation="get_policy",
            user_name=user_name,
            exc=exc,
        ) from exc


@router.put("/policies/{user_name}")
def save_policy(
    user_name: str,
    body: PolicyRequest,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Create or update a policy without allowing Live activation bypass."""

    user = _user_or_404(user_name)
    exchange = str(user.exchange).lower()
    if exchange not in {"hyperliquid", "bybit", "binance", "bitget"}:
        raise _logged_http_error(
            409,
            "Exchange has no Profit Sweep transfer adapter",
            operation="save_policy",
            user_name=user_name,
        )
    try:
        try:
            existing = _store().get_policy(user_name)
        except KeyError:
            values = {**default_policy(), **body.policy}
            if values.get("operating_mode") in {"live", "paused_unknown"}:
                raise ValueError("Use the Live activation endpoint to enable transfers")
            if exchange == "hyperliquid" and (not values.get("asset") or values.get("asset") == "USDT"):
                values["asset"] = "USDC"
            result = _store().create_policy(user_name, str(user.exchange), values)
        else:
            if existing["exchange"] != str(user.exchange):
                raise _logged_http_error(
                    409,
                    "Exchange changed; recreate the policy",
                    operation="save_policy",
                    user_name=user_name,
                )
            if body.expected_generation is None:
                raise ValueError("expected_generation is required when updating a policy")
            if body.expected_policy_fingerprint is None:
                raise ValueError("expected_policy_fingerprint is required when updating a policy")
            if body.expected_generation != existing["generation"]:
                raise ValueError("Policy changed in another request; reload before saving")
            if body.expected_policy_fingerprint != _policy_fingerprint(existing["policy"]):
                raise ValueError("Policy changed in another request; reload before saving")
            requested_mode = body.policy.get("operating_mode")
            current_mode = existing["policy"]["operating_mode"]
            if requested_mode == "paused_unknown" or (
                requested_mode == "live" and current_mode != "live"
            ):
                raise ValueError("Use the Live activation endpoint to enable or reconcile transfers")
            changed_fields = {
                key
                for key, value in body.policy.items()
                if existing["policy"].get(key) != value
            }
            baseline_fields = {"asset", "reference_capital", "baseline_mode"}
            if current_mode in {"live", "paused_unknown"} and changed_fields & baseline_fields:
                raise ValueError("Disable Live before changing settlement asset or baseline accounting")
            if (
                current_mode == "live"
                and requested_mode in {None, "live"}
                and changed_fields
                and not body.confirmed_live_update
            ):
                raise ValueError("Live policy changes require explicit confirmation")
            baseline_net_pnl = None
            if "asset" in changed_fields:
                merged = {**existing["policy"], **body.policy}
                now_ms = int(time.time() * 1000)
                lookback = int(merged.get("maximum_history_age") or 86_400)
                snapshot = collect_readonly_snapshot(
                    user,
                    max(0, now_ms - lookback * 1000),
                    now_ms,
                    30.0,
                    merged["asset"],
                )
                if not snapshot.get("complete"):
                    raise RuntimeError("New settlement-asset snapshot is incomplete")
                baseline_net_pnl, _cap = _transient_snapshot_pnl_and_cap(merged, snapshot)
            result = _store().update_policy(
                user_name,
                body.policy,
                baseline_net_pnl=baseline_net_pnl,
                expected_policy_fingerprint=body.expected_policy_fingerprint,
            )
    except HTTPException:
        raise
    except (ValueError, KeyError) as exc:
        raise _logged_http_error(
            422,
            str(exc),
            operation="save_policy",
            user_name=user_name,
            exc=exc,
        ) from exc
    except RuntimeError as exc:
        raise _logged_http_error(
            503,
            str(exc),
            operation="save_policy",
            user_name=user_name,
            exc=exc,
        ) from exc
    _log(
        SERVICE,
        f"Saved Profit Sweep policy for {user_name} in {result['policy']['operating_mode']} mode",
        level="INFO",
        user=user_name,
        meta={"operation": "save_policy", "exchange": str(user.exchange)},
    )
    return _public_policy_record(result)


@router.delete("/policies/{user_name}")
def delete_policy(
    user_name: str,
    body: DeletePolicyRequest,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Delete one local policy and its cascaded state and journals."""

    _user_or_404(user_name)
    try:
        deleted = _store().delete_policy(
            user_name,
            expected_policy_fingerprint=body.expected_policy_fingerprint,
        )
    except ValueError as exc:
        raise _logged_http_error(
            422,
            str(exc),
            operation="delete_policy",
            user_name=user_name,
            exc=exc,
        ) from exc
    if not deleted:
        raise _logged_http_error(
            404,
            "Profit Sweep policy not found",
            operation="delete_policy",
            user_name=user_name,
        )
    return {"ok": True}


@router.post("/live/{user_name}")
async def activate_live(
    user_name: str,
    body: LiveActivationRequest,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Activate Live from a fresh server-owned snapshot and stored baseline mode."""

    _user_or_404(user_name)
    try:
        result = await _run_account_operation(
            user_name,
            _activate_live_sync,
            user_name,
            body.expected_policy_fingerprint,
        )
    except KeyError as exc:
        raise _logged_http_error(
            404,
            "Profit Sweep policy not found",
            operation="activate_live",
            user_name=user_name,
            exc=exc,
        ) from exc
    except ValueError as exc:
        raise _logged_http_error(
            409,
            str(exc),
            operation="activate_live",
            user_name=user_name,
            exc=exc,
        ) from exc
    except RuntimeError as exc:
        raise _logged_http_error(
            503,
            str(exc),
            operation="activate_live",
            user_name=user_name,
            exc=exc,
        ) from exc
    _log(
        SERVICE,
        f"Activated Profit Sweep Live mode for {user_name}",
        level="INFO",
        user=user_name,
        meta={"operation": "activate_live", "exchange": result["policy"]["exchange"]},
    )
    wake = _SCHEDULER_WAKE
    if wake is not None:
        wake.set()
    return result


@router.get("/intents/{user_name}")
def get_intents(user_name: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return durable secret-free Live intents for one authenticated user."""

    _user_or_404(user_name)
    try:
        intents = _store().list_live_intents(user_name)
        leg_two_parents = {
            item.get("parent_id")
            for item in intents
            if item["leg"] == 2 and item["state"] != "failed"
        }
        return {"intents": [
            _public_live_intent(
                intent,
                can_reconcile=(
                    intent["state"] == "confirmed"
                    and intent["leg"] == 1
                    and intent["route"] == "vault_to_main_perps"
                    and intent.get("parent_id") is not None
                    and intent["operation_id"] != intent["parent_id"]
                    and intent["parent_id"] not in leg_two_parents
                ),
            )
            for intent in intents
        ]}
    except (KeyError, ValueError) as exc:
        raise _logged_http_error(
            404,
            "Profit Sweep policy not found",
            operation="list_intents",
            user_name=user_name,
            exc=exc,
        ) from exc


@router.post("/test-transfer/{user_name}")
async def test_transfer(
    user_name: str,
    body: TestTransferRequest | None = None,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Run one persisted manual internal transfer from a fresh snapshot."""

    try:
        result = await _run_account_operation(
            user_name,
            _test_transfer_sync,
            user_name,
            body.amount if body else "1",
            body.asset if body else "",
            body.operation_id if body else "",
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise _logged_http_error(
            409,
            str(exc),
            operation="test_transfer",
            user_name=user_name,
            exc=exc,
        ) from exc
    except RuntimeError as exc:
        raise _logged_http_error(
            503,
            str(exc),
            operation="test_transfer",
            user_name=user_name,
            exc=exc,
        ) from exc
    except Exception as exc:
        raise _logged_http_error(
            500,
            "Manual test transfer failed",
            operation="test_transfer",
            user_name=user_name,
            exc=exc,
        ) from exc
    _log(
        SERVICE,
        f"Manual test transfer for {user_name}: {result['status']}",
        level="INFO",
        user=user_name,
        meta={"operation": "test_transfer"},
    )
    return result


@router.post("/test-transfer/{user_name}/{operation_id}/back")
async def test_transfer_back(
    user_name: str,
    operation_id: str,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Reverse one confirmed manual test transfer without resubmitting its forward leg."""

    try:
        result = await _run_account_operation(
            user_name, _test_transfer_back_sync, user_name, operation_id
        )
    except HTTPException:
        raise
    except KeyError as exc:
        raise _logged_http_error(
            404,
            "Profit Sweep test operation not found",
            operation="test_transfer_back",
            user_name=user_name,
            exc=exc,
        ) from exc
    except ValueError as exc:
        raise _logged_http_error(
            409,
            str(exc),
            operation="test_transfer_back",
            user_name=user_name,
            exc=exc,
        ) from exc
    except RuntimeError as exc:
        raise _logged_http_error(
            503,
            str(exc),
            operation="test_transfer_back",
            user_name=user_name,
            exc=exc,
        ) from exc
    except Exception as exc:
        raise _logged_http_error(
            500,
            "Manual test transfer back failed",
            operation="test_transfer_back",
            user_name=user_name,
            exc=exc,
        ) from exc
    _log(
        SERVICE,
        f"Manual test transfer back for {user_name}: {result['status']}",
        level="INFO",
        user=user_name,
        meta={"operation": "test_transfer_back"},
    )
    return result


@router.post("/test-transfer/{user_name}/{operation_id}/signature")
async def submit_test_transfer_signature(
    user_name: str,
    operation_id: str,
    body: BrowserSignatureRequest,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Verify one foreground Leader-wallet signature and submit its prepared operation."""

    try:
        result = await _run_account_operation(
            user_name,
            _submit_test_signature_sync,
            user_name,
            operation_id,
            body.signature,
        )
    except HTTPException:
        raise
    except KeyError as exc:
        raise _logged_http_error(
            404,
            "Profit Sweep test operation not found",
            operation="submit_test_signature",
            user_name=user_name,
            exc=exc,
        ) from exc
    except ValueError as exc:
        raise _logged_http_error(
            409,
            str(exc),
            operation="submit_test_signature",
            user_name=user_name,
            exc=exc,
        ) from exc
    except RuntimeError as exc:
        raise _logged_http_error(
            503,
            str(exc),
            operation="submit_test_signature",
            user_name=user_name,
            exc=exc,
        ) from exc
    except Exception as exc:
        raise _logged_http_error(
            500,
            "Vault wallet signature submission failed",
            operation="submit_test_signature",
            user_name=user_name,
            exc=exc,
        ) from exc
    _log(
        SERVICE,
        f"Leader-wallet test transfer for {user_name}: {result['status']}",
        level="INFO",
        user=user_name,
        meta={"operation": "submit_test_signature"},
    )
    return result


@router.get("/test-transfers/{user_name}")
def get_test_transfers(
    user_name: str,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return visible manual test states without descriptors or routes."""

    _user_or_404(user_name)
    try:
        operations = _store().list_test_operations(user_name)
    except ValueError as exc:
        raise _logged_http_error(
            422,
            str(exc),
            operation="list_test_transfers",
            user_name=user_name,
            exc=exc,
        ) from exc
    returned = {
        item["parent_id"]
        for item in operations
        if item["direction"] == "back" and item.get("parent_id") is not None
    }
    return {
        "operations": [
            _public_test_operation(
                item,
                can_transfer_back=(
                    item["direction"] == "forward"
                    and item["state"] == "confirmed"
                    and item["operation_id"] not in returned
                ),
            )
            for item in operations
        ]
    }


@router.post("/reconcile/{user_name}/{operation_id}")
async def reconcile_intent(
    user_name: str,
    operation_id: str,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Retry only server-owned reconciliation for one persisted operation."""

    _user_or_404(user_name)
    try:
        intent = await _run_account_operation(
            user_name, _reconcile_operation_sync, user_name, operation_id
        )
    except KeyError as exc:
        raise _logged_http_error(
            404,
            "Profit Sweep intent not found",
            operation="reconcile_intent",
            user_name=user_name,
            exc=exc,
        ) from exc
    except (ValueError, RuntimeError) as exc:
        raise _logged_http_error(
            409,
            str(exc),
            operation="reconcile_intent",
            user_name=user_name,
            exc=exc,
        ) from exc
    return {
        "intent": _public_live_intent(intent),
        "policy": _public_policy_record(_store().get_policy(user_name)),
    }


@router.post("/evaluate/{user_name}")
async def evaluate_now(
    user_name: str,
    session: SessionToken = Depends(require_auth),
    body: PreviewRequest | None = None,
) -> dict[str, Any]:
    """Preview saved or unsaved form values without mutating policy state."""

    _user_or_404(user_name)
    try:
        return await _run_owned_thread(
            _preview_sync, user_name, body.policy if body is not None else None
        )
    except ValueError as exc:
        raise _logged_http_error(
            422,
            str(exc),
            operation="evaluate_preview",
            user_name=user_name,
            exc=exc,
        ) from exc
    except RuntimeError as exc:
        raise _logged_http_error(
            503,
            str(exc),
            operation="evaluate_preview",
            user_name=user_name,
            exc=exc,
        ) from exc


@router.post("/baseline/{user_name}")
def reset_baseline(
    user_name: str,
    body: BaselineRequest,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Reset both state generations from an explicit cumulative PnL value."""

    _user_or_404(user_name)
    try:
        return _public_policy_record(_store().reset_baseline(
            user_name,
            body.cumulative_net_pnl,
            expected_policy_fingerprint=body.expected_policy_fingerprint,
        ))
    except KeyError as exc:
        raise _logged_http_error(
            404,
            "Profit Sweep policy not found",
            operation="reset_baseline",
            user_name=user_name,
            exc=exc,
        ) from exc
    except ValueError as exc:
        raise _logged_http_error(
            422,
            str(exc),
            operation="reset_baseline",
            user_name=user_name,
            exc=exc,
        ) from exc


@router.get("/journal/{user_name}")
def get_journal(
    user_name: str,
    limit: int = Query(default=100, ge=1, le=500),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return recent Dry decisions for one exchange user."""

    _user_or_404(user_name)
    try:
        _store().get_policy(user_name)
        return {"journal": _store().list_simulation_journal(user_name, limit=limit)}
    except (KeyError, ValueError) as exc:
        raise _logged_http_error(
            404,
            "Profit Sweep policy not found",
            operation="get_journal",
            user_name=user_name,
            exc=exc,
        ) from exc


@router.get("/health")
def get_health(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return secret-free persistence, scheduler, and recovery diagnostics."""

    return {
        "feature_status": "live",
        "read_only": False,
        "scheduler_running": bool(_SCHEDULER_TASK is not None and not _SCHEDULER_TASK.done()),
        "database": _store().database_settings(),
    }
