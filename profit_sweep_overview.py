"""Persisted, read-only balance cache and API-owned overview refresh workers."""

from __future__ import annotations

import asyncio
from concurrent.futures import Future
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import time
import threading
import zlib
import hashlib
from typing import Any

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from secure_files import atomic_write_private_text, read_regular_file_nofollow
from profit_sweep_overview_balances import BalanceReader

SERVICE = "ProfitSweep"
REFRESH_SECONDS = 60
STALE_SECONDS = 180
DEFAULT_REFRESH_MINUTES = 15
REFRESH_MINUTES = (5, 15, 30, 60)


def load_refresh_minutes(path: Path) -> int:
    """Load the shared display interval without querying any exchange."""
    if not path.exists():
        return DEFAULT_REFRESH_MINUTES
    try:
        value = json.loads(read_regular_file_nofollow(path, path.parent))["refresh_minutes"]
        if type(value) is not int or value not in REFRESH_MINUTES:
            raise ValueError("Invalid overview interval")
        return value
    except (ValueError, KeyError, TypeError, OSError, RuntimeError):
        _log(SERVICE, "Invalid overview refresh settings; using 15 minutes", level="WARNING")
        return DEFAULT_REFRESH_MINUTES


def save_refresh_minutes(path: Path, value: int) -> None:
    """Persist the validated shared interval using a process lock and atomic replacement."""
    if type(value) is not int or value not in REFRESH_MINUTES:
        raise ValueError("Refresh interval must be 5, 15, 30 or 60 minutes")
    if path.is_symlink() or path.with_name(path.name + ".lock").is_symlink():
        raise ValueError("Overview settings must not be a symlink")
    with advisory_file_lock(path):
        atomic_write_private_text(path, json.dumps({"refresh_minutes": value}, indent=4))


def refresh_delay(name: str, cached: dict, interval: int) -> int:
    """Use the current interval, ignoring delays persisted by older versions."""
    failures = min(6, int(cached.get("failure_count", 0)))
    return min(interval * 4, interval * 2 ** max(0, failures - 1)) + zlib.crc32(name.encode("utf-8")) % 11


def needs_target_key_refresh(record: dict, cached: dict) -> bool:
    """Backfill legacy Hyperliquid target identities once, independently of the normal interval."""
    return (record["exchange"] == "hyperliquid" and not cached.get("target_key")
            and cached.get("target_key_checked") is not True)

_ISSUE_MESSAGES = {
    "unsupported_account_mode": "This account mode is not supported by the Standard/Manual transfer adapter.",
    "unsupported_exchange": "This exchange is not supported by the snapshot adapter.",
    "unsupported_asset": "The configured settlement asset is not supported.",
    "read_failed": "The exchange read failed or timed out.",
    "read_timeout": "The exchange request timed out after bounded retries.",
    "connection_error": "The exchange connection failed after bounded retries.",
    "rate_limited": "The exchange rate limit was reached (HTTP 429); overview requests are backing off.",
    "exchange_unavailable": "The exchange returned a temporary server error after bounded retries.",
    "invalid_response": "The exchange returned an unexpected response.",
    "invalid_wallet_address": "The configured public wallet address is invalid.",
    "agent_relationship_invalid": "The configured agent is missing or expired.",
    "snapshot_error": "The exchange snapshot is incomplete; see the account details.",
}
_ISSUE_SOURCES = frozenset({
    "userAbstraction", "userRole", "clearinghouseState", "spotClearinghouseState",
    "openOrders", "spotMeta", "userFillsByTime", "userFunding", "userNonFundingLedgerUpdates",
    "vaultDetails", "extraAgents", "userVaultEquities", "asset", "user", "history", "capability", "snapshot",
})


def snapshot_issues(snapshot: dict) -> list[dict]:
    """Retain allowlisted diagnostic codes, never raw exchange messages or payloads."""
    result = []
    errors = snapshot.get("errors") or []
    if not isinstance(errors, list):
        return result
    mode_read_failed = any(
        isinstance(error, dict) and error.get("source") == "userAbstraction"
        and error.get("code") in ("read_failed", "invalid_response")
        for error in errors
    )
    for error in errors[:20]:
        if not isinstance(error, dict):
            continue
        if mode_read_failed and error.get("code") == "unsupported_account_mode":
            # A failed mode read proves no incompatibility. Keep its actual read error.
            continue
        code = error.get("code")
        code = code if isinstance(code, str) and code in _ISSUE_MESSAGES else "snapshot_error"
        source = error.get("source")
        source = source if isinstance(source, str) and source in _ISSUE_SOURCES else "snapshot"
        issue = {"code": code, "source": source, "message": _ISSUE_MESSAGES[code]}
        if issue not in result:
            result.append(issue)
    return result


class TargetReadCache:
    """Coalesce concurrent reads of shared vault destinations for at most one minute."""

    def __init__(self):
        """Own a bounded set of short-lived read results and in-flight futures."""
        self.entries = {}
        self.lock = threading.Lock()

    def clear(self):
        """Release cached responses after all overview workers have stopped."""
        with self.lock:
            self.entries.clear()

    def read(self, key, fetch):
        """Share only display reads; unrelated keys execute without holding the lock."""
        with self.lock:
            now = time.monotonic()
            self.entries = {k: v for k, v in self.entries.items() if not v[1].done() or now - v[0] < REFRESH_SECONDS}
            entry = self.entries.get(key)
            owner = entry is None
            future = entry[1] if entry else Future()
            if owner and len(self.entries) < 512:
                self.entries[key] = (now, future)
        if owner:
            try:
                future.set_result(fetch())
            except Exception as exc:
                _log(SERVICE, f"Overview target read failed: {type(exc).__name__}", level="WARNING")
                future.set_result(None)
        return deepcopy(future.result())


def money(value: Any) -> str | None:
    """Keep only finite decimal values in the public and persisted projection."""
    try:
        result = Decimal(str(value))
        return format(result, "f") if result.is_finite() else None
    except (InvalidOperation, ValueError):
        return None


def balance_projection(snapshot: dict, policy: dict) -> dict:
    """Extract balances without retaining addresses, credentials, or raw responses."""
    balances = snapshot.get("account_balances") or {}
    source = balances.get("source") or {}
    target = balances.get("destination") or {}
    if snapshot.get("account_kind") == "vault":
        target = target.get(policy["vault_destination"]) or {}
    return {
        "balance": money(source.get("balance")),
        "target": str(target.get("label") or "")[:80],
        "target_balance": money(target.get("balance")),
        "target_key": target.get("overview_key"),
        "max_transferable": money(balances.get("max_transferable")),
        "updated_at": int(snapshot.get("collected_at_ms") or time.time() * 1000) // 1000,
    }


def cache_matches(record: dict, cached: dict) -> bool:
    """Invalidate cached balances when the generation, exchange, asset, or destination changes."""
    return (
        cached.get("generation") == record["generation"]
        and cached.get("exchange") == record["exchange"]
        and cached.get("asset") == record["policy"]["asset"]
        and cached.get("destination") == record["policy"]["vault_destination"]
    )


def overview_row(record: dict, cached: dict, last_sweep: int | None = None, refresh_seconds: int = 900) -> dict:
    """Combine durable financial state with independently dated balance data."""
    policy = record["policy"]
    mode = policy["operating_mode"]
    state = record["simulation_state" if mode == "dry" else "live_state"]
    if not cache_matches(record, cached):
        cached = {}
    reason = state.get("last_decision") or "not_evaluated"
    issues = snapshot_issues({"errors": cached.get("issues", [])})
    if mode == "paused_unknown":
        status, level = "Paused", "error"
    elif any(issue["code"] == "unsupported_account_mode" for issue in issues):
        status, level = "Account mode", "error"
    elif cached.get("error"):
        status, level = "Read error", "error"
    elif reason == "below_rounding_step":
        status, level = "Rounding", "waiting"
    elif reason == "below_high_watermark_recovery":
        status, level = "Recovery", "waiting"
    elif mode == "dry" and reason == "would_transfer":
        status, level = "Simulated", "ok"
    elif reason in {"confirmed", "transferred", "simulated_transfer"}:
        status, level = "OK", "ok"
    else:
        status, level = "Waiting", "waiting"
    recovery = max(Decimal("0"), Decimal(state["high_watermark"]) - Decimal(state["net_pnl"]))
    return {
        "name": record["user_name"], "mode": mode, "asset": policy["asset"],
        "status": status, "level": level, "reason": reason,
        "balance": cached.get("balance"), "target": cached.get("target") or "—",
        "target_balance": cached.get("target_balance"),
        "target_key": cached.get("target_key") or (
            hashlib.sha256((record["exchange"] + ":" + record["user_name"] + ":" + str(cached.get("target"))).encode()).hexdigest()
            if record["exchange"] != "hyperliquid" else None),
        "net_pnl": state["net_pnl"], "swept": state.get("simulated_total" if mode == "dry" else "confirmed_total", "0"),
        "sweep_due": state["sweep_due"], "recovery_needed": format(recovery, "f"),
        "reference_capital": policy["reference_capital"], "high_watermark": state["high_watermark"],
        "max_transferable": cached.get("max_transferable"), "last_sweep": last_sweep,
        "next_check": state.get("next_run_at"), "evaluated_at": state.get("last_evaluation_at"),
        "updated_at": cached.get("updated_at"), "read_error": bool(cached.get("error")),
        "issues": issues,
        "source_updated_at": cached.get("source_updated_at", cached.get("updated_at")),
        "target_updated_at": cached.get("target_updated_at", cached.get("updated_at")),
        "stale": not cached.get("updated_at") or time.time() - cached["updated_at"] > refresh_seconds + max(STALE_SECONDS, refresh_seconds // 5),
    }


class OverviewService:
    """Own bounded per-account refresh tasks and a restart-persistent balance cache."""

    refresh_requested_at = 0

    def request_refresh(self) -> None:
        """Coalesce manual requests for a minute and reuse the paced owned workers."""
        now = int(time.time())
        if now - self.refresh_requested_at >= 60:
            self.refresh_requested_at = now

    def __init__(self, store: Any, collect: Any, run_thread: Any):
        """Bind dependencies without starting work or querying an exchange."""
        self.store = store
        self.collect = collect
        self.run_thread = run_thread
        self.path = Path(store.db_path).with_name("overview.json")
        self.settings_path = self.path.with_name("overview_settings.json")
        self.refresh_seconds = load_refresh_minutes(self.settings_path) * 60
        self.cache = self._read()
        self.task = None
        self.workers: dict[str, asyncio.Task] = {}
        self.slots = asyncio.Semaphore(3)
        self.target_cache = TargetReadCache()
        self.balance_reader = BalanceReader()

    def _read(self) -> dict:
        """Read an optional private cache, logging malformed data explicitly."""
        if not self.path.exists():
            return {}
        try:
            data = json.loads(read_regular_file_nofollow(self.path, self.path.parent))
            if not isinstance(data, dict) or not all(isinstance(v, dict) for v in data.values()):
                raise ValueError("Invalid overview cache")
            for value in data.values():
                for field in ("generation", "updated_at", "attempted_at", "failure_count", "refresh_delay"):
                    if field in value and (type(value[field]) is not int or value[field] < 0):
                        raise ValueError("Invalid overview cache timestamp or generation")
                for field in ("balance", "target_balance", "max_transferable"):
                    if value.get(field) is not None and money(value[field]) is None:
                        raise ValueError("Invalid overview cache balance")
                for field in ("exchange", "asset", "destination", "target"):
                    if field in value and not isinstance(value[field], str):
                        raise ValueError("Invalid overview cache label")
            return data
        except (ValueError, OSError, RuntimeError):
            _log(SERVICE, "Overview cache could not be read; balances will be refreshed", level="WARNING")
            return {}

    def _persist(self, name: str, value: dict, active: set[str]) -> None:
        """Merge one completion atomically under the shared reentrant process lock."""
        if self.path.is_symlink() or self.path.with_name(self.path.name + ".lock").is_symlink():
            raise ValueError("Overview cache must not be a symlink")
        with advisory_file_lock(self.path):
            data = {k: v for k, v in self._read().items() if k in active}
            if name in active:
                data[name] = value
            atomic_write_private_text(self.path, json.dumps(data, indent=4))

    def start(self) -> None:
        """Start exactly one supervisor on the owning API event loop."""
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._loop(), name="profit-sweep-overview")

    async def stop(self) -> None:
        """Cancel and await the supervisor and all owned account workers."""
        self.balance_reader.stop()
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        tasks = list(self.workers.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.workers.clear()
        self.cache.clear()
        self.target_cache.clear()
        self.task = None

    async def _refresh(self, record: dict) -> None:
        """Refresh one account without evaluating policies or submitting transfers."""
        name = record["user_name"]
        async with self.slots:
            try:
                snapshot = await self.run_thread(self.collect, record)
                value = balance_projection(snapshot, record["policy"])
                value["error"] = bool(snapshot.get("errors")) or value["balance"] is None
                value["issues"] = snapshot_issues(snapshot)
                if value["error"]:
                    summary = ", ".join(issue["source"] + ": " + issue["code"] for issue in value["issues"])
                    _log(SERVICE, "Overview snapshot issue: " + summary, level="WARNING", user=name)
                old = self.cache.get(name, {})
                for field, stamp in (("balance", "source_updated_at"), ("target_balance", "target_updated_at")):
                    value[stamp] = value["updated_at"] if value[field] is not None else None
                    if value[field] is None and cache_matches(record, old) and value["error"]:
                        value[field] = old.get(field)
                        value[stamp] = old.get(stamp, old.get("updated_at")) if value[field] is not None else None
                        if field == "target_balance" and not value["target"]:
                            value["target"] = old.get("target", "")
                        if field == "target_balance":
                            value["target_key"] = value.get("target_key") or old.get("target_key")
                dates = [value[k] for k in ("source_updated_at", "target_updated_at") if value.get(k) is not None]
                value["updated_at"] = min(dates) if dates else 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log(SERVICE, f"Overview refresh failed: {type(exc).__name__}", level="WARNING", user=name)
                old = self.cache.get(name, {})
                value = {**(old if cache_matches(record, old) else {}), "error": True,
                         "issues": snapshot_issues({"errors": [{"code": "read_failed", "source": "snapshot"}]})}
            current = await self.run_thread(self.store.get_policy, name)
            if current["generation"] != record["generation"] or current["policy"] != record["policy"]:
                return
            value.update(generation=record["generation"], exchange=record["exchange"],
                         asset=record["policy"]["asset"], destination=record["policy"]["vault_destination"],
                         attempted_at=int(time.time()), target_key_checked=True)
            previous = self.cache.get(name, {})
            failures = min(6, int(previous.get("failure_count", 0)) + 1) if value["error"] else 0
            value["failure_count"] = failures
            value["refresh_delay"] = refresh_delay(name, value, self.refresh_seconds)
            self.cache[name] = value
            records = await self.run_thread(self.store.list_policies)
            active = {r["user_name"] for r in records if r["policy"]["operating_mode"] != "disabled"}
            await self.run_thread(self._persist, name, value, active)

    async def _loop(self) -> None:
        """Schedule independent bounded refreshes; one slow account cannot stall the rest."""
        while True:
            try:
                self.refresh_seconds = 60 * await self.run_thread(load_refresh_minutes, self.settings_path)
                for name, task in list(self.workers.items()):
                    if task.done():
                        self.workers.pop(name)
                        if not task.cancelled() and task.exception() is not None:
                            _log(SERVICE, "Overview account refresh could not complete", level="WARNING", user=name)
                records = await self.run_thread(self.store.list_policies)
                active = {r["user_name"] for r in records if r["policy"]["operating_mode"] != "disabled"}
                removed = set(self.cache) - active
                self.cache = {k: v for k, v in self.cache.items() if k in active}
                if removed:
                    await self.run_thread(self._persist, "", {}, active)
                for name, task in self.workers.items():
                    if name not in active and not task.cancelling():
                        task.cancel()
                for record in records:
                    name = record["user_name"]
                    cached = self.cache.get(name, {})
                    if name in active and name not in self.workers and (
                        not cache_matches(record, cached)
                        or cached.get("attempted_at", 0) < self.refresh_requested_at
                        or needs_target_key_refresh(record, cached)
                        or time.time() - cached.get("attempted_at", 0) >= refresh_delay(name, cached, self.refresh_seconds)
                    ):
                        self.workers[name] = asyncio.create_task(self._refresh(record), name="sweep-overview-account")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log(SERVICE, f"Overview scheduling failed: {type(exc).__name__}", level="WARNING")
            await asyncio.sleep(5)
