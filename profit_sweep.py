"""Persistent exchange-neutral policy and dry-run logic for profit sweeps."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Mapping

from file_lock import advisory_file_lock


SERVICE = "ProfitSweep"
SCHEMA_VERSION = 5
BUSY_TIMEOUT_MS = 5_000
DEFAULT_DB_PATH = Path(__file__).resolve().parent / "data" / "state" / "profit_sweep" / "profit_sweep.sqlite3"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _@:+-]{0,127}$")
_OPERATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_ASSET_RE = re.compile(r"^[A-Z0-9]{2,16}$")
_DECIMAL_MAX = Decimal("1000000000000000000")

_DEFAULT_POLICY: dict[str, Any] = {
    "operating_mode": "disabled",
    "asset": "USDT",
    "reference_capital": "0",
    "baseline_mode": "from_enable",
    "trigger_percent": "10",
    "sweep_percent": "5",
    "minimum_transfer_amount": "25",
    "simulation_minimum_transfer_amount": "25",
    "live_minimum_transfer_amount": "25",
    "safety_reserve_mode": "fixed",
    "safety_reserve_amount": "0",
    "safety_reserve_percent": "0",
    "daily_transfer_limit_enabled": False,
    "daily_transfer_limit": "0",
    "single_transfer_limit_enabled": False,
    "single_transfer_limit": "0",
    "trigger_mode": "hybrid",
    "periodic_interval": 21_600,
    "settlement_debounce": 900,
    "quiet_period": 300,
    "stabilization_interval": 60,
    "successful_transfer_cooldown": 3_600,
    "vault_transfer_cooldown": 21_600,
    "schedule_jitter_percent": "10",
    "maximum_history_age": 86_400,
    "maximum_preflight_age": 60,
    "live_activation_baseline_mode": "fresh",
    "first_live_catchup_limit_enabled": False,
    "first_live_catchup_limit": "0",
    "vault_withdraw_mode": "flat_only",
    "vault_destination": "main_perps",
    "vault_minimum_transfer_amount": "50",
    "retained_leader_equity": "100",
    "share_safety_buffer": "0.01",
    "vault_safety_reserve_mode": "fixed",
    "vault_safety_reserve_amount": "100",
    "vault_safety_reserve_percent": "0",
    "vault_conditional_cost_policy": "pause_on_cost_or_forced_close",
    "main_destination_activity_policy": "warn",
}

_ENUMS = {
    "operating_mode": {"disabled", "dry", "live", "paused_unknown"},
    "baseline_mode": {"from_enable", "lifetime"},
    "safety_reserve_mode": {"fixed", "percent", "max_of_both"},
    "trigger_mode": {"hybrid", "interval"},
    "live_activation_baseline_mode": {"fresh", "include_dry_period"},
    "vault_withdraw_mode": {"flat_only", "margin_buffered"},
    "vault_destination": {"main_perps", "main_spot"},
    "vault_safety_reserve_mode": {"fixed", "percent", "max_of_both"},
    "vault_conditional_cost_policy": {"pause_on_cost_or_forced_close"},
    "main_destination_activity_policy": {"warn", "pause_future_sweeps"},
}

_BOOLEANS = {
    "daily_transfer_limit_enabled",
    "single_transfer_limit_enabled",
    "first_live_catchup_limit_enabled",
}

_DECIMAL_RANGES = {
    "reference_capital": (Decimal("0"), _DECIMAL_MAX),
    "trigger_percent": (Decimal("0"), Decimal("100")),
    "sweep_percent": (Decimal("0"), Decimal("100")),
    "minimum_transfer_amount": (Decimal("0"), _DECIMAL_MAX),
    "simulation_minimum_transfer_amount": (Decimal("0"), _DECIMAL_MAX),
    "live_minimum_transfer_amount": (Decimal("0"), _DECIMAL_MAX),
    "safety_reserve_amount": (Decimal("0"), _DECIMAL_MAX),
    "safety_reserve_percent": (Decimal("0"), Decimal("100")),
    "daily_transfer_limit": (Decimal("0"), _DECIMAL_MAX),
    "single_transfer_limit": (Decimal("0"), _DECIMAL_MAX),
    "schedule_jitter_percent": (Decimal("0"), Decimal("50")),
    "first_live_catchup_limit": (Decimal("0"), _DECIMAL_MAX),
    "vault_minimum_transfer_amount": (Decimal("0"), _DECIMAL_MAX),
    "retained_leader_equity": (Decimal("100"), _DECIMAL_MAX),
    "share_safety_buffer": (Decimal("0"), Decimal("0.45")),
    "vault_safety_reserve_amount": (Decimal("0"), _DECIMAL_MAX),
    "vault_safety_reserve_percent": (Decimal("0"), Decimal("100")),
}

_INTEGER_RANGES = {
    "periodic_interval": (60, 31_536_000),
    "settlement_debounce": (0, 86_400),
    "quiet_period": (0, 86_400),
    "stabilization_interval": (0, 86_400),
    "successful_transfer_cooldown": (0, 2_592_000),
    "vault_transfer_cooldown": (0, 2_592_000),
    "maximum_history_age": (60, 31_536_000),
    "maximum_preflight_age": (1, 86_400),
}

_STATE_TABLES = {"simulation": "simulation_state", "live": "live_state"}
_STATE_TOTAL_COLUMNS = {"simulation": "simulated_total", "live": "confirmed_total"}


def default_policy() -> dict[str, Any]:
    """Return a fresh JSON-safe copy of every Profit Sweep policy default."""
    return deepcopy(_DEFAULT_POLICY)


def _decimal(value: Any, field: str, minimum: Decimal = -_DECIMAL_MAX, maximum: Decimal = _DECIMAL_MAX) -> Decimal:
    """Parse one exact decimal input without accepting binary floats or integers."""
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, str) and value == value.strip() and value:
        try:
            parsed = Decimal(value)
        except InvalidOperation as exc:
            raise ValueError(f"{field} must be a decimal string") from exc
    else:
        raise ValueError(f"{field} must be a decimal string")
    if not parsed.is_finite() or parsed < minimum or parsed > maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return parsed


def _decimal_text(value: Decimal) -> str:
    """Return a non-exponent canonical decimal string suitable for JSON and SQLite."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _validate_identifier(value: Any, field: str) -> str:
    """Validate a persisted user or exchange identifier at the storage boundary."""
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{field} contains unsupported characters")
    if value in {".", ".."} or any(character in value for character in ("/", "\\", "\x00")):
        raise ValueError(f"{field} contains unsupported characters")
    return value


def _normalize_policy(values: Mapping[str, Any] | None, base: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Merge and strictly validate a complete or partial policy mapping."""
    if values is None:
        values = {}
    if not isinstance(values, Mapping):
        raise ValueError("policy must be a mapping")
    unknown = set(values) - set(_DEFAULT_POLICY)
    if unknown:
        raise ValueError(f"unknown policy fields: {', '.join(sorted(unknown))}")
    normalized = deepcopy(dict(base or _DEFAULT_POLICY))
    normalized.update(values)

    if "minimum_transfer_amount" in values:
        if "simulation_minimum_transfer_amount" not in values:
            normalized["simulation_minimum_transfer_amount"] = values["minimum_transfer_amount"]
        if "live_minimum_transfer_amount" not in values:
            normalized["live_minimum_transfer_amount"] = values["minimum_transfer_amount"]

    for field, allowed in _ENUMS.items():
        value = normalized[field]
        if not isinstance(value, str) or value not in allowed:
            raise ValueError(f"{field} must be one of: {', '.join(sorted(allowed))}")
    asset = normalized["asset"]
    if not isinstance(asset, str) or not _ASSET_RE.fullmatch(asset):
        raise ValueError("asset must be an uppercase settlement asset")
    for field in _BOOLEANS:
        if type(normalized[field]) is not bool:
            raise ValueError(f"{field} must be a boolean")
    for field, (minimum, maximum) in _DECIMAL_RANGES.items():
        normalized[field] = _decimal_text(_decimal(normalized[field], field, minimum, maximum))
    for field, (minimum, maximum) in _INTEGER_RANGES.items():
        value = normalized[field]
        if type(value) is not int or value < minimum or value > maximum:
            raise ValueError(f"{field} must be an integer between {minimum} and {maximum}")
    for enabled_field, limit_field in (
        ("daily_transfer_limit_enabled", "daily_transfer_limit"),
        ("single_transfer_limit_enabled", "single_transfer_limit"),
        ("first_live_catchup_limit_enabled", "first_live_catchup_limit"),
    ):
        if normalized[enabled_field] and Decimal(normalized[limit_field]) <= 0:
            raise ValueError(f"{limit_field} must be greater than zero when enabled")
    return normalized


def _canonical_json(value: Any) -> str:
    """Encode immutable metadata canonically while rejecting non-JSON and float values."""
    def validate(item: Any) -> None:
        """Reject imprecise or unsupported values recursively."""
        if item is None or type(item) in {str, int, bool}:
            return
        if isinstance(item, list):
            for child in item:
                validate(child)
            return
        if isinstance(item, dict) and all(isinstance(key, str) for key in item):
            for child in item.values():
                validate(child)
            return
        raise ValueError("payload must contain only JSON-safe values and decimal strings")

    validate(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _policy_fingerprint(policy: Mapping[str, Any]) -> str:
    """Return a stable optimistic-concurrency fingerprint for one policy."""

    return hashlib.sha256(_canonical_json(dict(policy)).encode("ascii")).hexdigest()


def _reserve_amount(policy: Mapping[str, Any]) -> Decimal:
    """Calculate the configured fixed, percentage, or combined safety reserve."""
    fixed = Decimal(policy["safety_reserve_amount"])
    percent = Decimal(policy["reference_capital"]) * Decimal(policy["safety_reserve_percent"]) / Decimal("100")
    if policy["safety_reserve_mode"] == "fixed":
        return fixed
    if policy["safety_reserve_mode"] == "percent":
        return percent
    return max(fixed, percent)


def calculate_sweep(
    policy: Mapping[str, Any],
    *,
    net_pnl: str | Decimal,
    high_watermark: str | Decimal,
    sweep_due: str | Decimal,
    max_transferable: str | Decimal,
    transferred_today: str | Decimal = "0",
    state_kind: str = "simulation",
    minimum_transfer_override: str | Decimal | None = None,
) -> dict[str, Any]:
    """Calculate one JSON-safe HWM decision without mutating persistent state."""
    normalized = _normalize_policy(policy)
    if state_kind not in _STATE_TABLES:
        raise ValueError("state_kind must be simulation or live")
    pnl = _decimal(net_pnl, "net_pnl")
    hwm = _decimal(high_watermark, "high_watermark", Decimal("0"))
    due = _decimal(sweep_due, "sweep_due", Decimal("0"))
    transferable = _decimal(max_transferable, "max_transferable", Decimal("0"))
    daily_used = _decimal(transferred_today, "transferred_today", Decimal("0"))

    trigger = Decimal(normalized["reference_capital"]) * Decimal(normalized["trigger_percent"]) / Decimal("100")
    new_peak = Decimal("0")
    if pnl >= trigger:
        new_peak = max(Decimal("0"), pnl - hwm)
        due += new_peak * Decimal(normalized["sweep_percent"]) / Decimal("100")
        hwm = max(hwm, pnl)

    reserve = _reserve_amount(normalized)
    available = max(Decimal("0"), transferable - reserve)
    limits = [due, available]
    if normalized["single_transfer_limit_enabled"]:
        limits.append(Decimal(normalized["single_transfer_limit"]))
    if normalized["daily_transfer_limit_enabled"]:
        limits.append(max(Decimal("0"), Decimal(normalized["daily_transfer_limit"]) - daily_used))
    amount = max(Decimal("0"), min(limits))
    recovered_to_peak = pnl >= hwm
    minimum_field = "simulation_minimum_transfer_amount" if state_kind == "simulation" else "live_minimum_transfer_amount"
    minimum = (
        _decimal(minimum_transfer_override, "minimum_transfer_override", Decimal("0"))
        if minimum_transfer_override is not None
        else Decimal(normalized[minimum_field])
    )
    if pnl < trigger or not recovered_to_peak or amount < minimum:
        amount = Decimal("0")

    if pnl < trigger:
        reason = "trigger_not_reached"
    elif not recovered_to_peak:
        reason = "below_high_watermark_recovery"
    elif due == 0:
        reason = "no_new_peak_profit"
    elif amount == 0 and min(limits) < minimum:
        reason = "below_minimum_or_cap"
    elif amount > 0:
        reason = "would_transfer"
    else:
        reason = "no_transferable_amount"
    return {
        "net_pnl": _decimal_text(pnl),
        "trigger_amount": _decimal_text(trigger),
        "new_peak_profit": _decimal_text(new_peak),
        "high_watermark": _decimal_text(hwm),
        "sweep_due": _decimal_text(due),
        "safety_reserve": _decimal_text(reserve),
        "effective_cap": _decimal_text(min(limits[1:]) if len(limits) > 1 else Decimal("0")),
        "amount": _decimal_text(amount),
        "would_transfer": amount > 0,
        "reason": reason,
    }


class ProfitSweepStore:
    """Own the private profit-sweep policy, financial state, and intent journal."""

    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH, busy_timeout_ms: int = BUSY_TIMEOUT_MS):
        """Open or initialize a versioned owner-only SQLite database."""
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be a positive integer")
        raw_path = Path(db_path).expanduser()
        self.db_path = raw_path if raw_path.is_absolute() else Path.cwd() / raw_path
        self.lock_path = self.db_path.with_name(f"{self.db_path.name}.lock")
        self.busy_timeout_ms = busy_timeout_ms
        self._prepare_paths()
        self._initialize()

    def _prepare_paths(self) -> None:
        """Create the dedicated private directory and reject symlinked storage paths."""
        current = self.db_path.parent
        while current != current.parent:
            if current.exists() and current.is_symlink():
                raise ValueError("profit sweep database parent must not be a symlink")
            current = current.parent
        self.db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.db_path.parent.stat().st_uid != os.getuid():
            raise PermissionError("profit sweep database directory must be owned by the current user")
        os.chmod(self.db_path.parent, 0o700)
        for path in (self.db_path, self.lock_path):
            if path.is_symlink():
                raise ValueError("profit sweep database and lock must not be symlinks")
            if not path.exists():
                descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(descriptor)
            os.chmod(path, 0o600)

    def _secure_files(self) -> None:
        """Enforce owner-only modes on the database, lock, and SQLite sidecars."""
        for path in (
            self.db_path,
            self.lock_path,
            self.db_path.with_name(f"{self.db_path.name}-wal"),
            self.db_path.with_name(f"{self.db_path.name}-shm"),
        ):
            try:
                if not path.exists():
                    continue
                if path.is_symlink():
                    raise ValueError("profit sweep SQLite files must not be symlinks")
                if path.stat().st_uid != os.getuid():
                    raise PermissionError("profit sweep SQLite files must be owned by the current user")
                os.chmod(path, 0o600)
            except FileNotFoundError:
                # SQLite removes WAL/SHM sidecars when the final connection closes.
                continue

    def _connect(self) -> sqlite3.Connection:
        """Return a hardened SQLite connection with mandatory durability settings."""
        if self.db_path.is_symlink():
            raise ValueError("profit sweep database must not be a symlink")
        connection = sqlite3.connect(self.db_path, timeout=self.busy_timeout_ms / 1000)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        if str(mode).lower() != "wal":
            connection.close()
            raise RuntimeError("profit sweep database requires SQLite WAL mode")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        self._secure_files()
        return connection

    def _initialize(self) -> None:
        """Create or transactionally migrate the schema without replacing state."""
        with advisory_file_lock(self.db_path):
            self._secure_files()
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                version = int(connection.execute("PRAGMA user_version").fetchone()[0])
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                    )
                }
                if version == 0 and tables:
                    raise RuntimeError("unversioned profit sweep schema is not supported")
                if version not in {0, 1, 2, 3, 4, SCHEMA_VERSION}:
                    raise RuntimeError(f"unsupported profit sweep schema version: {version}")
                if version == 0:
                    self._create_schema(connection)
                    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                elif version == 1:
                    self._migrate_v1_to_v2(connection)
                    self._migrate_v2_to_v3(connection)
                    self._migrate_v3_to_v4(connection)
                    self._migrate_v4_to_v5(connection)
                    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                elif version == 2:
                    self._migrate_v2_to_v3(connection)
                    self._migrate_v3_to_v4(connection)
                    self._migrate_v4_to_v5(connection)
                    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                elif version == 3:
                    self._migrate_v3_to_v4(connection)
                    self._migrate_v4_to_v5(connection)
                    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                elif version == 4:
                    self._migrate_v4_to_v5(connection)
                    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
                self._secure_files()

    def _create_schema(self, connection: sqlite3.Connection) -> None:
        """Create all tables using TEXT for every financial value."""
        statements = (
            """CREATE TABLE policies (
                user_name TEXT PRIMARY KEY,
                exchange TEXT NOT NULL,
                config_json TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK (generation > 0),
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )""",
            """CREATE TABLE simulation_state (
                user_name TEXT PRIMARY KEY REFERENCES policies(user_name) ON DELETE CASCADE,
                generation INTEGER NOT NULL,
                baseline_pnl TEXT NOT NULL,
                net_pnl TEXT NOT NULL DEFAULT '0',
                high_watermark TEXT NOT NULL DEFAULT '0',
                sweep_due TEXT NOT NULL DEFAULT '0',
                simulated_total TEXT NOT NULL DEFAULT '0',
                daily_date TEXT,
                daily_total TEXT NOT NULL DEFAULT '0',
                last_evaluation_at INTEGER,
                next_run_at INTEGER,
                last_event_at INTEGER,
                last_successful_scan_at INTEGER,
                last_decision TEXT
            )""",
            """CREATE TABLE live_state (
                user_name TEXT PRIMARY KEY REFERENCES policies(user_name) ON DELETE CASCADE,
                generation INTEGER NOT NULL,
                baseline_pnl TEXT NOT NULL,
                net_pnl TEXT NOT NULL DEFAULT '0',
                high_watermark TEXT NOT NULL DEFAULT '0',
                sweep_due TEXT NOT NULL DEFAULT '0',
                confirmed_total TEXT NOT NULL DEFAULT '0',
                daily_date TEXT,
                daily_total TEXT NOT NULL DEFAULT '0',
                last_evaluation_at INTEGER,
                next_run_at INTEGER,
                last_event_at INTEGER,
                last_successful_scan_at INTEGER,
                last_decision TEXT,
                active_baseline_mode TEXT NOT NULL DEFAULT 'legacy_unknown'
            )""",
            """CREATE TABLE ledger_events (
                user_name TEXT NOT NULL REFERENCES policies(user_name) ON DELETE CASCADE,
                exchange TEXT NOT NULL,
                event_time_ms INTEGER NOT NULL,
                event_hash TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                asset TEXT NOT NULL,
                realized_trade_pnl TEXT NOT NULL,
                funding TEXT NOT NULL,
                fees TEXT NOT NULL,
                exchange_corrections TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (user_name, exchange, event_time_ms, event_hash, event_type)
            )""",
            """CREATE TABLE simulation_journal (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                user_name TEXT NOT NULL REFERENCES policies(user_name) ON DELETE CASCADE,
                generation INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                net_pnl TEXT NOT NULL,
                high_watermark TEXT NOT NULL,
                due_before TEXT NOT NULL,
                due_after TEXT NOT NULL,
                amount TEXT NOT NULL,
                safety_reserve TEXT NOT NULL,
                effective_cap TEXT NOT NULL,
                reason TEXT NOT NULL
            )""",
            "CREATE INDEX ledger_events_user_time ON ledger_events(user_name, exchange, event_time_ms)",
            "CREATE INDEX simulation_journal_user_time ON simulation_journal(user_name, created_at)",
        )
        for statement in statements:
            connection.execute(statement)
        self._create_live_intents_schema(connection)
        self._create_test_operations_schema(connection)

    def _create_live_intents_schema(self, connection: sqlite3.Connection) -> None:
        """Create the durable live-intent state machine and unresolved guard."""

        connection.execute(
            """CREATE TABLE live_intents (
                operation_id TEXT PRIMARY KEY,
                user_name TEXT NOT NULL REFERENCES policies(user_name) ON DELETE CASCADE,
                parent_id TEXT,
                leg INTEGER NOT NULL CHECK (leg > 0),
                route TEXT NOT NULL,
                descriptor_json TEXT NOT NULL,
                submission_json TEXT,
                state TEXT NOT NULL CHECK (state IN ('prepared', 'submitting', 'confirmed', 'failed', 'unknown')),
                reserved_amount TEXT NOT NULL,
                prepared_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                submitted_at INTEGER,
                resolved_at INTEGER,
                error_json TEXT
            )"""
        )
        connection.execute(
            """CREATE UNIQUE INDEX live_intents_one_unresolved ON live_intents(user_name)
               WHERE state IN ('prepared', 'submitting', 'unknown')"""
        )
        connection.execute(
            "CREATE INDEX live_intents_user_time ON live_intents(user_name, prepared_at, operation_id)"
        )
        connection.execute(
            "CREATE INDEX live_intents_parent ON live_intents(parent_id, leg)"
        )

    def _migrate_v1_to_v2(self, connection: sqlite3.Connection) -> None:
        """Add live intents to a version-one database without rewriting existing rows."""

        self._create_live_intents_schema(connection)

    def _create_test_operations_schema(self, connection: sqlite3.Connection) -> None:
        """Create the isolated manual transfer journal with no financial-state links."""

        connection.execute(
            """CREATE TABLE test_operations (
                operation_id TEXT PRIMARY KEY,
                user_name TEXT NOT NULL,
                parent_id TEXT,
                direction TEXT NOT NULL CHECK (direction IN ('forward', 'back')),
                route TEXT NOT NULL,
                descriptor_json TEXT NOT NULL,
                submission_json TEXT,
                state TEXT NOT NULL CHECK (state IN ('prepared', 'submitting', 'confirmed', 'failed', 'unknown')),
                requested_amount TEXT NOT NULL,
                actual_amount TEXT,
                prepared_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                submitted_at INTEGER,
                resolved_at INTEGER,
                error_json TEXT
            )"""
        )
        connection.execute(
            """CREATE UNIQUE INDEX test_operations_one_back ON test_operations(parent_id)
               WHERE direction = 'back' AND parent_id IS NOT NULL"""
        )
        connection.execute(
            "CREATE INDEX test_operations_user_time ON test_operations(user_name, prepared_at, operation_id)"
        )

    def _migrate_v2_to_v3(self, connection: sqlite3.Connection) -> None:
        """Add isolated manual transfer operations without changing sweep state."""

        self._create_test_operations_schema(connection)

    def _migrate_v3_to_v4(self, connection: sqlite3.Connection) -> None:
        """Bind ledger uniqueness to exchange identity and reject conflicting history."""

        rows = connection.execute(
            "SELECT * FROM ledger_events ORDER BY created_at, rowid"
        ).fetchall()
        retained: dict[tuple[Any, ...], sqlite3.Row] = {}
        for row in rows:
            identity = (
                row["user_name"],
                row["exchange"],
                row["event_time_ms"],
                row["event_hash"],
                row["event_type"],
            )
            immutable = (
                row["payload_json"],
                row["asset"],
                row["realized_trade_pnl"],
                row["funding"],
                row["fees"],
                row["exchange_corrections"],
            )
            existing = retained.get(identity)
            if existing is None:
                retained[identity] = row
                continue
            existing_immutable = (
                existing["payload_json"],
                existing["asset"],
                existing["realized_trade_pnl"],
                existing["funding"],
                existing["fees"],
                existing["exchange_corrections"],
            )
            if immutable != existing_immutable:
                raise RuntimeError("conflicting duplicate ledger event identity blocks schema migration")

        connection.execute(
            """CREATE TABLE ledger_events_v4 (
                user_name TEXT NOT NULL REFERENCES policies(user_name) ON DELETE CASCADE,
                exchange TEXT NOT NULL,
                event_time_ms INTEGER NOT NULL,
                event_hash TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                asset TEXT NOT NULL,
                realized_trade_pnl TEXT NOT NULL,
                funding TEXT NOT NULL,
                fees TEXT NOT NULL,
                exchange_corrections TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (user_name, exchange, event_time_ms, event_hash, event_type)
            )"""
        )
        columns = (
            "user_name", "exchange", "event_time_ms", "event_hash", "event_type",
            "payload_hash", "payload_json", "asset", "realized_trade_pnl", "funding",
            "fees", "exchange_corrections", "created_at",
        )
        placeholders = ", ".join("?" for _ in columns)
        for row in retained.values():
            connection.execute(
                f"INSERT INTO ledger_events_v4 ({', '.join(columns)}) VALUES ({placeholders})",
                tuple(row[column] for column in columns),
            )
        connection.execute("DROP TABLE ledger_events")
        connection.execute("ALTER TABLE ledger_events_v4 RENAME TO ledger_events")
        connection.execute(
            "CREATE INDEX ledger_events_user_time ON ledger_events(user_name, exchange, event_time_ms)"
        )

    @staticmethod
    def _migrate_v4_to_v5(connection: sqlite3.Connection) -> None:
        """Track the baseline mode actually applied to the current Live state."""

        columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(live_state)").fetchall()
        }
        if "active_baseline_mode" in columns:
            return
        connection.execute(
            "ALTER TABLE live_state ADD COLUMN active_baseline_mode TEXT NOT NULL DEFAULT 'legacy_unknown'"
        )

    def _write(self, callback: Any) -> Any:
        """Run one callback under an advisory process lock and BEGIN IMMEDIATE."""
        with advisory_file_lock(self.db_path):
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                result = callback(connection)
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
                self._secure_files()

    def _read(self, callback: Any) -> Any:
        """Run one read callback on a short-lived hardened connection."""
        connection = self._connect()
        try:
            return callback(connection)
        finally:
            connection.close()
            self._secure_files()

    def _policy_row(self, connection: sqlite3.Connection, user_name: str) -> sqlite3.Row:
        """Return one policy row or raise a stable lookup error."""
        row = connection.execute("SELECT * FROM policies WHERE user_name = ?", (user_name,)).fetchone()
        if row is None:
            raise KeyError(f"profit sweep policy not found: {user_name}")
        return row

    def _state_dict(self, row: sqlite3.Row, state_kind: str) -> dict[str, Any]:
        """Convert one simulation or live state row to a JSON-safe dictionary."""
        total_column = _STATE_TOTAL_COLUMNS[state_kind]
        result = {
            "state_kind": state_kind,
            "generation": row["generation"],
            "baseline_pnl": row["baseline_pnl"],
            "net_pnl": row["net_pnl"],
            "high_watermark": row["high_watermark"],
            "sweep_due": row["sweep_due"],
            total_column: row[total_column],
            "daily_date": row["daily_date"],
            "daily_total": row["daily_total"],
            "last_evaluation_at": row["last_evaluation_at"],
            "next_run_at": row["next_run_at"],
            "last_event_at": row["last_event_at"],
            "last_successful_scan_at": row["last_successful_scan_at"],
            "last_decision": row["last_decision"],
        }
        if state_kind == "live":
            result["active_baseline_mode"] = row["active_baseline_mode"]
        return result

    def _intent_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        """Convert one persisted live intent to a JSON-safe public record."""

        return {
            "operation_id": row["operation_id"],
            "user_name": row["user_name"],
            "parent_id": row["parent_id"],
            "leg": row["leg"],
            "route": row["route"],
            "descriptor": json.loads(row["descriptor_json"]),
            "submission": json.loads(row["submission_json"]) if row["submission_json"] else None,
            "state": row["state"],
            "reserved_amount": row["reserved_amount"],
            "prepared_at": row["prepared_at"],
            "updated_at": row["updated_at"],
            "submitted_at": row["submitted_at"],
            "resolved_at": row["resolved_at"],
            "error": json.loads(row["error_json"]) if row["error_json"] else None,
        }

    def _intent_row(self, connection: sqlite3.Connection, operation_id: str) -> sqlite3.Row:
        """Return one intent row or raise a stable lookup error."""

        row = connection.execute(
            "SELECT * FROM live_intents WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"profit sweep intent not found: {operation_id}")
        return row

    def _test_operation_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        """Convert one isolated test operation to a JSON-safe record."""

        return {
            "operation_id": row["operation_id"],
            "user_name": row["user_name"],
            "parent_id": row["parent_id"],
            "direction": row["direction"],
            "route": row["route"],
            "descriptor": json.loads(row["descriptor_json"]),
            "submission": json.loads(row["submission_json"]) if row["submission_json"] else None,
            "state": row["state"],
            "requested_amount": row["requested_amount"],
            "actual_amount": row["actual_amount"],
            "prepared_at": row["prepared_at"],
            "updated_at": row["updated_at"],
            "submitted_at": row["submitted_at"],
            "resolved_at": row["resolved_at"],
            "error": json.loads(row["error_json"]) if row["error_json"] else None,
        }

    def _test_operation_row(self, connection: sqlite3.Connection, operation_id: str) -> sqlite3.Row:
        """Return one isolated test operation or raise a stable lookup error."""

        row = connection.execute(
            "SELECT * FROM test_operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"profit sweep test operation not found: {operation_id}")
        return row

    def _policy_dict(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        """Build one complete JSON-safe policy response including separate states."""
        simulation = connection.execute("SELECT * FROM simulation_state WHERE user_name = ?", (row["user_name"],)).fetchone()
        live = connection.execute("SELECT * FROM live_state WHERE user_name = ?", (row["user_name"],)).fetchone()
        return {
            "user_name": row["user_name"],
            "exchange": row["exchange"],
            "generation": row["generation"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "policy": json.loads(row["config_json"]),
            "simulation_state": self._state_dict(simulation, "simulation"),
            "live_state": self._state_dict(live, "live"),
        }

    def _insert_states(self, connection: sqlite3.Connection, user_name: str, generation: int, baseline: str) -> None:
        """Insert fresh isolated simulation and live state rows."""
        connection.execute(
            "INSERT INTO simulation_state (user_name, generation, baseline_pnl) VALUES (?, ?, ?)",
            (user_name, generation, baseline),
        )
        connection.execute(
            "INSERT INTO live_state (user_name, generation, baseline_pnl) VALUES (?, ?, ?)",
            (user_name, generation, baseline),
        )

    def create_policy(
        self,
        user_name: str,
        exchange: str,
        policy: Mapping[str, Any] | None = None,
        *,
        baseline_net_pnl: str | Decimal = "0",
    ) -> dict[str, Any]:
        """Create a validated policy and both generation-one states."""
        user = _validate_identifier(user_name, "user_name")
        normalized_exchange = _validate_identifier(exchange, "exchange")
        normalized = _normalize_policy(policy)
        baseline_input = _decimal(baseline_net_pnl, "baseline_net_pnl")
        baseline = baseline_input if normalized["baseline_mode"] == "from_enable" else Decimal("0")
        now = int(time.time())

        def create(connection: sqlite3.Connection) -> dict[str, Any]:
            """Insert the policy atomically with its two state rows."""
            connection.execute(
                "INSERT INTO policies (user_name, exchange, config_json, generation, created_at, updated_at) VALUES (?, ?, ?, 1, ?, ?)",
                (user, normalized_exchange, _canonical_json(normalized), now, now),
            )
            self._insert_states(connection, user, 1, _decimal_text(baseline))
            return self._policy_dict(connection, self._policy_row(connection, user))

        return self._write(create)

    def get_policy(self, user_name: str) -> dict[str, Any]:
        """Return one policy and its separate simulation/live states."""
        user = _validate_identifier(user_name, "user_name")
        return self._read(lambda connection: self._policy_dict(connection, self._policy_row(connection, user)))

    def list_policies(self) -> list[dict[str, Any]]:
        """Return all policies in stable user-name order."""
        def load(connection: sqlite3.Connection) -> list[dict[str, Any]]:
            """Load all policy rows from one consistent read connection."""
            rows = connection.execute("SELECT * FROM policies ORDER BY user_name").fetchall()
            return [self._policy_dict(connection, row) for row in rows]

        return self._read(load)

    def update_policy(
        self,
        user_name: str,
        changes: Mapping[str, Any],
        *,
        baseline_net_pnl: str | Decimal | None = None,
        expected_policy_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        """Update a policy, resetting generation for accounting-identity edits."""
        user = _validate_identifier(user_name, "user_name")
        if not isinstance(changes, Mapping) or not changes:
            raise ValueError("changes must be a non-empty mapping")

        def update(connection: sqlite3.Connection) -> dict[str, Any]:
            """Apply one validated policy update within the write transaction."""
            row = self._policy_row(connection, user)
            current = json.loads(row["config_json"])
            if (
                expected_policy_fingerprint is not None
                and _policy_fingerprint(current) != expected_policy_fingerprint
            ):
                raise ValueError("policy changed before update")
            normalized = _normalize_policy(changes, current)
            changed_fields = {
                key for key in changes if normalized.get(key) != current.get(key)
            }
            unresolved = connection.execute(
                """SELECT operation_id FROM live_intents WHERE user_name = ?
                   AND state IN ('prepared', 'submitting', 'unknown')""",
                (user,),
            ).fetchone()
            if unresolved is not None and normalized["operating_mode"] != current["operating_mode"]:
                raise ValueError("an unresolved live intent blocks operating mode changes")
            baseline_edit = bool({"asset", "reference_capital", "baseline_mode"} & changed_fields)
            if unresolved is not None and baseline_edit:
                raise ValueError("an unresolved live intent blocks baseline changes")
            generation = int(row["generation"])
            if baseline_edit:
                simulation = connection.execute(
                    "SELECT baseline_pnl, net_pnl FROM simulation_state WHERE user_name = ?", (user,)
                ).fetchone()
                inferred = Decimal(simulation["baseline_pnl"]) + Decimal(simulation["net_pnl"])
                supplied = inferred if baseline_net_pnl is None else _decimal(baseline_net_pnl, "baseline_net_pnl")
                baseline = supplied if normalized["baseline_mode"] == "from_enable" else Decimal("0")
                generation += 1
                connection.execute("DELETE FROM simulation_state WHERE user_name = ?", (user,))
                connection.execute("DELETE FROM live_state WHERE user_name = ?", (user,))
                self._insert_states(connection, user, generation, _decimal_text(baseline))
            now = int(time.time())
            connection.execute(
                "UPDATE policies SET config_json = ?, generation = ?, updated_at = ? WHERE user_name = ?",
                (_canonical_json(normalized), generation, now, user),
            )
            return self._policy_dict(connection, self._policy_row(connection, user))

        return self._write(update)

    def reset_baseline(
        self,
        user_name: str,
        baseline_net_pnl: str | Decimal,
        *,
        expected_policy_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        """Explicitly start a new generation and reset both isolated states."""
        user = _validate_identifier(user_name, "user_name")
        supplied = _decimal(baseline_net_pnl, "baseline_net_pnl")

        def reset(connection: sqlite3.Connection) -> dict[str, Any]:
            """Replace both state rows with the next generation."""
            row = self._policy_row(connection, user)
            policy = json.loads(row["config_json"])
            if policy["operating_mode"] in {"live", "paused_unknown"}:
                raise ValueError("Disable Live before resetting its accounting baseline")
            if (
                expected_policy_fingerprint is not None
                and _policy_fingerprint(policy) != expected_policy_fingerprint
            ):
                raise ValueError("policy changed after baseline confirmation")
            unresolved = connection.execute(
                """SELECT 1 FROM live_intents WHERE user_name = ?
                   AND state IN ('prepared', 'submitting', 'unknown')""",
                (user,),
            ).fetchone()
            if unresolved is not None:
                raise ValueError("an unresolved live intent blocks baseline reset")
            baseline = supplied if policy["baseline_mode"] == "from_enable" else Decimal("0")
            generation = int(row["generation"]) + 1
            connection.execute("DELETE FROM simulation_state WHERE user_name = ?", (user,))
            connection.execute("DELETE FROM live_state WHERE user_name = ?", (user,))
            self._insert_states(connection, user, generation, _decimal_text(baseline))
            connection.execute(
                "UPDATE policies SET generation = ?, updated_at = ? WHERE user_name = ?",
                (generation, int(time.time()), user),
            )
            return self._policy_dict(connection, self._policy_row(connection, user))

        return self._write(reset)

    def activate_live(
        self,
        user_name: str,
        cumulative_net_pnl: str | Decimal,
        *,
        baseline_mode: str | None = None,
        expected_policy_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        """Activate Live with a fresh or Dry-generation baseline in one transaction."""

        user = _validate_identifier(user_name, "user_name")
        cumulative = _decimal(cumulative_net_pnl, "cumulative_net_pnl")

        def activate(connection: sqlite3.Connection) -> dict[str, Any]:
            """Validate activation and replace only the isolated Live state."""

            row = self._policy_row(connection, user)
            policy = json.loads(row["config_json"])
            if (
                expected_policy_fingerprint is not None
                and _policy_fingerprint(policy) != expected_policy_fingerprint
            ):
                raise ValueError("policy changed after Live confirmation")
            if policy["operating_mode"] not in {"disabled", "dry"}:
                raise ValueError("Live activation requires a disabled or dry policy")
            mode = baseline_mode or policy["live_activation_baseline_mode"]
            if mode not in {"fresh", "include_dry_period"}:
                raise ValueError("unsupported live activation baseline mode")
            unresolved = connection.execute(
                """SELECT 1 FROM live_intents WHERE user_name = ?
                   AND state IN ('prepared', 'submitting', 'unknown')""",
                (user,),
            ).fetchone()
            if unresolved is not None:
                raise ValueError("an unresolved live intent blocks activation")
            simulation = connection.execute(
                "SELECT baseline_pnl FROM simulation_state WHERE user_name = ?", (user,)
            ).fetchone()
            baseline = cumulative if mode == "fresh" else Decimal(simulation["baseline_pnl"])
            connection.execute("DELETE FROM live_state WHERE user_name = ?", (user,))
            connection.execute(
                """INSERT INTO live_state (
                    user_name, generation, baseline_pnl, active_baseline_mode
                ) VALUES (?, ?, ?, ?)""",
                (user, row["generation"], _decimal_text(baseline), mode),
            )
            policy["operating_mode"] = "live"
            connection.execute(
                "UPDATE policies SET config_json = ?, updated_at = ? WHERE user_name = ?",
                (_canonical_json(policy), int(time.time()), user),
            )
            return self._policy_dict(connection, self._policy_row(connection, user))

        return self._write(activate)

    def rebaseline_live(
        self,
        user_name: str,
        cumulative_net_pnl: str | Decimal,
        baseline_mode: str,
        policy_changes: Mapping[str, Any],
        *,
        expected_policy_fingerprint: str,
        recover_legacy_dry_generation: bool = False,
    ) -> dict[str, Any]:
        """Recalculate an active Live baseline before any Live transfer has completed."""

        user = _validate_identifier(user_name, "user_name")
        cumulative = _decimal(cumulative_net_pnl, "cumulative_net_pnl")
        if baseline_mode not in {"fresh", "include_dry_period"}:
            raise ValueError("unsupported live baseline mode")

        def rebaseline(connection: sqlite3.Connection) -> dict[str, Any]:
            """Validate and replace the untouched Live accounting state atomically."""

            row = self._policy_row(connection, user)
            policy = json.loads(row["config_json"])
            if _policy_fingerprint(policy) != expected_policy_fingerprint:
                raise ValueError("policy changed before Live baseline recalculation")
            if policy["operating_mode"] != "live":
                raise ValueError("Live baseline recalculation requires operating_mode=live")
            unresolved = connection.execute(
                """SELECT 1 FROM live_intents WHERE user_name = ?
                   AND state IN ('prepared', 'submitting', 'unknown')""",
                (user,),
            ).fetchone()
            if unresolved is not None:
                raise ValueError("an unresolved Live intent blocks baseline recalculation")
            confirmed_intent = connection.execute(
                "SELECT 1 FROM live_intents WHERE user_name = ? AND state = 'confirmed'",
                (user,),
            ).fetchone()
            live = connection.execute(
                "SELECT * FROM live_state WHERE user_name = ?", (user,)
            ).fetchone()
            if Decimal(live["confirmed_total"]) != 0 or confirmed_intent is not None:
                raise ValueError("confirmed Live transfers block retroactive baseline recalculation")

            normalized = _normalize_policy(policy_changes, policy)
            normalized["operating_mode"] = "live"
            normalized["live_activation_baseline_mode"] = baseline_mode
            baseline = cumulative
            if baseline_mode == "include_dry_period":
                simulation = connection.execute(
                    "SELECT * FROM simulation_state WHERE user_name = ?", (user,)
                ).fetchone()
                baseline = Decimal(simulation["baseline_pnl"])
                legacy_empty_state = (
                    live["active_baseline_mode"] == "legacy_unknown"
                    and simulation["last_evaluation_at"] is None
                    and Decimal(simulation["net_pnl"]) == 0
                    and Decimal(simulation["high_watermark"]) == 0
                    and Decimal(simulation["sweep_due"]) == 0
                )
                if recover_legacy_dry_generation:
                    if policy["baseline_mode"] != "from_enable" or not legacy_empty_state:
                        raise ValueError("previous Dry generation cannot be recovered from this Live state")
                    previous = connection.execute(
                        """SELECT generation, net_pnl FROM simulation_journal
                           WHERE user_name = ? AND generation < ?
                           ORDER BY sequence DESC LIMIT 1""",
                        (user, simulation["generation"]),
                    ).fetchone()
                    if previous is None:
                        raise ValueError("no previous Dry generation is available for recovery")
                    baseline -= Decimal(previous["net_pnl"])

            timestamp = int(time.time())
            connection.execute(
                """UPDATE live_state SET baseline_pnl = ?, net_pnl = '0', high_watermark = '0',
                   sweep_due = '0', confirmed_total = '0', daily_date = NULL, daily_total = '0',
                   last_evaluation_at = NULL, next_run_at = ?, last_event_at = NULL,
                   last_successful_scan_at = NULL, last_decision = 'baseline_recalculated',
                   active_baseline_mode = ? WHERE user_name = ?""",
                (_decimal_text(baseline), timestamp, baseline_mode, user),
            )
            connection.execute(
                "UPDATE policies SET config_json = ?, updated_at = ? WHERE user_name = ?",
                (_canonical_json(normalized), timestamp, user),
            )
            return self._policy_dict(connection, self._policy_row(connection, user))

        return self._write(rebaseline)

    def delete_policy(
        self,
        user_name: str,
        *,
        expected_policy_fingerprint: str | None = None,
    ) -> bool:
        """Delete one policy and its cascaded state and simulation journal."""
        user = _validate_identifier(user_name, "user_name")

        def delete(connection: sqlite3.Connection) -> bool:
            """Delete one policy row and report whether it existed."""
            row = connection.execute(
                "SELECT config_json FROM policies WHERE user_name = ?", (user,)
            ).fetchone()
            if row is None:
                return False
            policy = json.loads(row["config_json"])
            if policy["operating_mode"] in {"live", "paused_unknown"}:
                raise ValueError("Disable Live before deleting its accounting history")
            if (
                expected_policy_fingerprint is not None
                and _policy_fingerprint(policy) != expected_policy_fingerprint
            ):
                raise ValueError("policy changed after delete confirmation")
            unresolved = connection.execute(
                """SELECT 1 FROM live_intents WHERE user_name = ?
                   AND state IN ('prepared', 'submitting', 'unknown')""",
                (user,),
            ).fetchone()
            if unresolved is not None:
                raise ValueError("an unresolved live intent blocks policy deletion")
            cursor = connection.execute("DELETE FROM policies WHERE user_name = ?", (user,))
            return cursor.rowcount == 1

        return self._write(delete)

    def upsert_ledger_event(
        self,
        *,
        user_name: str,
        exchange: str,
        event_time_ms: int,
        event_hash: str,
        event_type: str,
        asset: str,
        realized_trade_pnl: str | Decimal = "0",
        funding: str | Decimal = "0",
        fees: str | Decimal = "0",
        exchange_corrections: str | Decimal = "0",
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Insert one immutable normalized ledger event or deduplicate its exact key."""
        user = _validate_identifier(user_name, "user_name")
        normalized_exchange = _validate_identifier(exchange, "exchange")
        if type(event_time_ms) is not int or event_time_ms < 0:
            raise ValueError("event_time_ms must be a non-negative integer")
        normalized_hash = _validate_identifier(event_hash, "event_hash")
        normalized_type = _validate_identifier(event_type, "event_type")
        if not isinstance(asset, str) or not _ASSET_RE.fullmatch(asset):
            raise ValueError("asset must be an uppercase settlement asset")
        payload_json = _canonical_json(dict(payload or {}))
        payload_hash = hashlib.sha256(payload_json.encode("ascii")).hexdigest()
        financial = {
            "realized_trade_pnl": _decimal_text(_decimal(realized_trade_pnl, "realized_trade_pnl")),
            "funding": _decimal_text(_decimal(funding, "funding")),
            "fees": _decimal_text(_decimal(fees, "fees")),
            "exchange_corrections": _decimal_text(_decimal(exchange_corrections, "exchange_corrections")),
        }
        identity = (user, normalized_exchange, event_time_ms, normalized_hash, normalized_type)

        def insert(connection: sqlite3.Connection) -> dict[str, Any]:
            """Insert or verify one event without ever updating immutable columns."""
            cursor = connection.execute(
                """INSERT OR IGNORE INTO ledger_events (
                    user_name, exchange, event_time_ms, event_hash, event_type, payload_hash, payload_json,
                    asset, realized_trade_pnl, funding, fees, exchange_corrections, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (*identity, payload_hash, payload_json, asset, *financial.values(), int(time.time())),
            )
            row = connection.execute(
                """SELECT * FROM ledger_events WHERE user_name = ? AND exchange = ? AND event_time_ms = ?
                   AND event_hash = ? AND event_type = ?""",
                identity,
            ).fetchone()
            immutable = (
                row["payload_json"], row["asset"], row["realized_trade_pnl"], row["funding"],
                row["fees"], row["exchange_corrections"],
            )
            expected = (payload_json, asset, *financial.values())
            if immutable != expected:
                raise ValueError("immutable ledger event conflicts with an existing exchange identity")
            return self._ledger_dict(row, inserted=cursor.rowcount == 1)

        return self._write(insert)

    def _ledger_dict(self, row: sqlite3.Row, *, inserted: bool | None = None) -> dict[str, Any]:
        """Convert a normalized ledger row to a JSON-safe dictionary."""
        result = {
            "user_name": row["user_name"],
            "exchange": row["exchange"],
            "event_time_ms": row["event_time_ms"],
            "event_hash": row["event_hash"],
            "event_type": row["event_type"],
            "asset": row["asset"],
            "realized_trade_pnl": row["realized_trade_pnl"],
            "funding": row["funding"],
            "fees": row["fees"],
            "exchange_corrections": row["exchange_corrections"],
            "payload": json.loads(row["payload_json"]),
            "created_at": row["created_at"],
        }
        if inserted is not None:
            result["inserted"] = inserted
        return result

    def list_ledger_events(
        self,
        user_name: str,
        exchange: str,
        asset: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return normalized immutable events, optionally limited to one settlement asset."""
        user = _validate_identifier(user_name, "user_name")
        normalized_exchange = _validate_identifier(exchange, "exchange")
        if asset is not None and (not isinstance(asset, str) or not _ASSET_RE.fullmatch(asset)):
            raise ValueError("asset must be an uppercase settlement asset")

        def load(connection: sqlite3.Connection) -> list[dict[str, Any]]:
            """Load all matching ledger events."""
            asset_clause = " AND asset = ?" if asset is not None else ""
            parameters = (user, normalized_exchange, asset) if asset is not None else (user, normalized_exchange)
            rows = connection.execute(
                f"""SELECT * FROM ledger_events WHERE user_name = ? AND exchange = ?{asset_clause}
                    ORDER BY event_time_ms, event_hash, event_type""",
                parameters,
            ).fetchall()
            return [self._ledger_dict(row) for row in rows]

        return self._read(load)

    def ledger_net_pnl(self, user_name: str, exchange: str, asset: str | None = None) -> str:
        """Sum one settlement asset, defaulting safely to the policy-configured asset."""
        user = _validate_identifier(user_name, "user_name")
        normalized_exchange = _validate_identifier(exchange, "exchange")
        if asset is not None and (not isinstance(asset, str) or not _ASSET_RE.fullmatch(asset)):
            raise ValueError("asset must be an uppercase settlement asset")

        def total(connection: sqlite3.Connection) -> str:
            """Resolve the policy asset and sum its rows in one read snapshot."""

            connection.execute("BEGIN")
            selected_asset = asset
            if selected_asset is None:
                policy = json.loads(self._policy_row(connection, user)["config_json"])
                selected_asset = policy["asset"]
            rows = connection.execute(
                """SELECT realized_trade_pnl, funding, fees, exchange_corrections
                   FROM ledger_events WHERE user_name = ? AND exchange = ? AND asset = ?""",
                (user, normalized_exchange, selected_asset),
            ).fetchall()
            value = sum(
                (
                    Decimal(row["realized_trade_pnl"])
                    + Decimal(row["funding"])
                    - Decimal(row["fees"])
                    + Decimal(row["exchange_corrections"])
                    for row in rows
                ),
                Decimal("0"),
            )
            return _decimal_text(value)

        return self._read(total)

    def evaluate_dry(
        self,
        user_name: str,
        *,
        cumulative_net_pnl: str | Decimal,
        max_transferable: str | Decimal,
        minimum_transfer_override: str | Decimal | None = None,
        now: int | None = None,
        commit: bool = True,
    ) -> dict[str, Any]:
        """Evaluate and optionally journal a dry decision without allocating live artifacts."""
        user = _validate_identifier(user_name, "user_name")
        raw_pnl = _decimal(cumulative_net_pnl, "cumulative_net_pnl")
        transferable = _decimal(max_transferable, "max_transferable", Decimal("0"))
        timestamp = int(time.time()) if now is None else now
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("now must be a non-negative integer Unix timestamp")

        def evaluate(connection: sqlite3.Connection) -> dict[str, Any]:
            """Calculate against simulation state and optionally commit only simulation changes."""
            policy_row = self._policy_row(connection, user)
            policy = json.loads(policy_row["config_json"])
            if policy["operating_mode"] != "dry":
                raise ValueError("dry evaluation requires operating_mode=dry")
            simulation = connection.execute("SELECT * FROM simulation_state WHERE user_name = ?", (user,)).fetchone()
            live = connection.execute("SELECT confirmed_total FROM live_state WHERE user_name = ?", (user,)).fetchone()
            policy_pnl = raw_pnl - Decimal(simulation["baseline_pnl"])
            day = datetime.fromtimestamp(timestamp, tz=timezone.utc).date().isoformat()
            daily_total = Decimal(simulation["daily_total"]) if simulation["daily_date"] == day else Decimal("0")
            decision = calculate_sweep(
                policy,
                net_pnl=policy_pnl,
                high_watermark=simulation["high_watermark"],
                sweep_due=simulation["sweep_due"],
                max_transferable=transferable,
                transferred_today=daily_total,
                state_kind="simulation",
                minimum_transfer_override=minimum_transfer_override,
            )
            due_before_simulation = Decimal(decision["sweep_due"])
            amount = Decimal(decision["amount"])
            due_after = due_before_simulation - amount
            simulated_total = Decimal(simulation["simulated_total"]) + amount
            daily_after = daily_total + amount
            result = {
                **decision,
                "state_kind": "simulation",
                "generation": simulation["generation"],
                "baseline_pnl": simulation["baseline_pnl"],
                "sweep_due_before_simulation": _decimal_text(due_before_simulation),
                "sweep_due_after_simulation": _decimal_text(due_after),
                "simulated_total": _decimal_text(simulated_total),
                "confirmed_total": live["confirmed_total"],
                "committed": commit,
            }
            if commit:
                connection.execute(
                    """UPDATE simulation_state SET net_pnl = ?, high_watermark = ?, sweep_due = ?,
                       simulated_total = ?, daily_date = ?, daily_total = ?, last_evaluation_at = ?, last_decision = ?
                       WHERE user_name = ?""",
                    (
                        decision["net_pnl"],
                        decision["high_watermark"],
                        _decimal_text(due_after),
                        _decimal_text(simulated_total),
                        day,
                        _decimal_text(daily_after),
                        timestamp,
                        decision["reason"],
                        user,
                    ),
                )
                connection.execute(
                    """INSERT INTO simulation_journal (
                        user_name, generation, created_at, net_pnl, high_watermark, due_before,
                        due_after, amount, safety_reserve, effective_cap, reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        user,
                        simulation["generation"],
                        timestamp,
                        decision["net_pnl"],
                        decision["high_watermark"],
                        _decimal_text(due_before_simulation),
                        _decimal_text(due_after),
                        decision["amount"],
                        decision["safety_reserve"],
                        decision["effective_cap"],
                        decision["reason"],
                    ),
                )
            return result

        return self._write(evaluate) if commit else self._read(evaluate)

    def evaluate_live(
        self,
        user_name: str,
        *,
        cumulative_net_pnl: str | Decimal,
        max_transferable: str | Decimal,
        minimum_transfer_override: str | Decimal | None = None,
        now: int | None = None,
        commit: bool = True,
    ) -> dict[str, Any]:
        """Evaluate Live state while leaving due reserved until confirmation."""

        user = _validate_identifier(user_name, "user_name")
        raw_pnl = _decimal(cumulative_net_pnl, "cumulative_net_pnl")
        transferable = _decimal(max_transferable, "max_transferable", Decimal("0"))
        timestamp = int(time.time()) if now is None else now
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("now must be a non-negative integer Unix timestamp")

        def evaluate(connection: sqlite3.Connection) -> dict[str, Any]:
            """Calculate and optionally persist one Live HWM/due transition."""

            policy_row = self._policy_row(connection, user)
            policy = json.loads(policy_row["config_json"])
            if policy["operating_mode"] != "live":
                raise ValueError("live evaluation requires operating_mode=live")
            unresolved = connection.execute(
                """SELECT operation_id FROM live_intents WHERE user_name = ?
                   AND state IN ('prepared', 'submitting', 'unknown')""",
                (user,),
            ).fetchone()
            if unresolved is not None:
                raise ValueError(f"unresolved live intent blocks evaluation: {unresolved['operation_id']}")
            live = connection.execute("SELECT * FROM live_state WHERE user_name = ?", (user,)).fetchone()
            policy_pnl = raw_pnl - Decimal(live["baseline_pnl"])
            day = datetime.fromtimestamp(timestamp, tz=timezone.utc).date().isoformat()
            daily_total = Decimal(live["daily_total"]) if live["daily_date"] == day else Decimal("0")
            decision = calculate_sweep(
                policy,
                net_pnl=policy_pnl,
                high_watermark=live["high_watermark"],
                sweep_due=live["sweep_due"],
                max_transferable=transferable,
                transferred_today=daily_total,
                state_kind="live",
                minimum_transfer_override=minimum_transfer_override,
            )
            amount = Decimal(decision["amount"])
            if (
                amount > 0
                and policy["first_live_catchup_limit_enabled"]
                and Decimal(live["confirmed_total"]) == 0
            ):
                amount = min(amount, Decimal(policy["first_live_catchup_limit"]))
            minimum = (
                _decimal(minimum_transfer_override, "minimum_transfer_override", Decimal("0"))
                if minimum_transfer_override is not None
                else Decimal(policy["live_minimum_transfer_amount"])
            )
            if amount < minimum:
                amount = Decimal("0")
            reason = (
                "below_minimum_or_cap"
                if amount == 0 and Decimal(decision["amount"]) > 0
                else decision["reason"]
            )
            result = {
                **decision,
                "amount": _decimal_text(amount),
                "would_transfer": amount > 0,
                "reason": reason,
                "state_kind": "live",
                "generation": live["generation"],
                "baseline_pnl": live["baseline_pnl"],
                "confirmed_total": live["confirmed_total"],
                "reserved_total": "0",
                "reservation_guard": {
                    "generation": live["generation"],
                    "sweep_due": decision["sweep_due"],
                    "daily_date": day,
                    "daily_total": _decimal_text(daily_total),
                    "last_evaluation_at": timestamp,
                },
                "committed": commit,
            }
            if commit:
                connection.execute(
                    """UPDATE live_state SET net_pnl = ?, high_watermark = ?, sweep_due = ?,
                       daily_date = ?, daily_total = ?, last_evaluation_at = ?, last_decision = ?
                       WHERE user_name = ?""",
                    (
                        decision["net_pnl"],
                        decision["high_watermark"],
                        decision["sweep_due"],
                        day,
                        _decimal_text(daily_total),
                        timestamp,
                        reason,
                        user,
                    ),
                )
            return result

        return self._write(evaluate) if commit else self._read(evaluate)

    def create_live_intent(
        self,
        user_name: str,
        *,
        operation_id: str,
        parent_id: str | None,
        leg: int,
        route: str,
        descriptor: Mapping[str, Any],
        reserved_amount: str | Decimal,
        reservation_guard: Mapping[str, Any] | None = None,
        now: int | None = None,
    ) -> dict[str, Any]:
        """Persist one prepared reservation before any external submission."""

        user = _validate_identifier(user_name, "user_name")
        operation = self._validate_operation_id(operation_id, "operation_id")
        parent = self._validate_operation_id(parent_id, "parent_id") if parent_id is not None else None
        normalized_route = self._validate_operation_id(route, "route")
        if type(leg) is not int or leg < 1:
            raise ValueError("leg must be a positive integer")
        amount = _decimal(reserved_amount, "reserved_amount", Decimal("0"))
        if amount <= 0:
            raise ValueError("reserved_amount must be greater than zero")
        descriptor_value = dict(descriptor)
        if (
            descriptor_value.get("operation_id") != operation
            or descriptor_value.get("route") != normalized_route
            or _decimal(descriptor_value.get("amount"), "descriptor amount", Decimal("0")) != amount
        ):
            raise ValueError("descriptor does not match the intent reservation")
        descriptor_json = _canonical_json(descriptor_value)
        guard = dict(reservation_guard) if reservation_guard is not None else None
        timestamp = int(time.time()) if now is None else now
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("now must be a non-negative integer Unix timestamp")

        def create(connection: sqlite3.Connection) -> dict[str, Any]:
            """Reserve due by creating the sole unresolved intent."""

            policy = json.loads(self._policy_row(connection, user)["config_json"])
            recovery_leg = (
                leg == 2
                and parent is not None
                and normalized_route == "main_perps_to_spot"
            )
            if policy["operating_mode"] != "live" and not (
                policy["operating_mode"] == "paused_unknown" and recovery_leg
            ):
                raise ValueError("prepared intents require operating_mode=live")
            if recovery_leg:
                confirmed_parent = connection.execute(
                    """SELECT 1 FROM live_intents WHERE parent_id = ? AND leg = 1
                       AND route = 'vault_to_main_perps' AND state = 'confirmed'""",
                    (parent,),
                ).fetchone()
                if confirmed_parent is None:
                    raise ValueError("Vault forwarding recovery requires a confirmed first leg")
            live = connection.execute("SELECT * FROM live_state WHERE user_name = ?", (user,)).fetchone()
            if guard is not None:
                expected = (
                    guard.get("generation"),
                    str(guard.get("sweep_due")),
                    guard.get("daily_date"),
                    str(guard.get("daily_total")),
                    guard.get("last_evaluation_at"),
                )
                current = (
                    live["generation"],
                    live["sweep_due"],
                    live["daily_date"],
                    live["daily_total"],
                    live["last_evaluation_at"],
                )
                if expected != current:
                    raise ValueError("live reservation decision is stale")
            if amount > Decimal(live["sweep_due"]):
                raise ValueError("reserved_amount exceeds live sweep due")
            connection.execute(
                """INSERT INTO live_intents (
                    operation_id, user_name, parent_id, leg, route, descriptor_json, state,
                    reserved_amount, prepared_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'prepared', ?, ?, ?)""",
                (
                    operation,
                    user,
                    parent,
                    leg,
                    normalized_route,
                    descriptor_json,
                    _decimal_text(amount),
                    timestamp,
                    timestamp,
                ),
            )
            return self._intent_dict(self._intent_row(connection, operation))

        return self._write(create)

    @staticmethod
    def _validate_operation_id(value: Any, field: str) -> str:
        """Validate a stable intent, parent, or fixed-route identifier."""

        if not isinstance(value, str) or not _OPERATION_ID_RE.fullmatch(value):
            raise ValueError(f"{field} is invalid")
        return value

    def get_live_intent(self, operation_id: str) -> dict[str, Any]:
        """Return one durable intent by stable operation ID."""

        operation = self._validate_operation_id(operation_id, "operation_id")
        return self._read(lambda connection: self._intent_dict(self._intent_row(connection, operation)))

    def list_live_intents(
        self,
        user_name: str,
        *,
        unresolved_only: bool = False,
    ) -> list[dict[str, Any]]:
        """List one policy's intents in deterministic operation order."""

        user = _validate_identifier(user_name, "user_name")

        def load(connection: sqlite3.Connection) -> list[dict[str, Any]]:
            """Load matching intent rows from one connection."""

            self._policy_row(connection, user)
            where = " AND state IN ('prepared', 'submitting', 'unknown')" if unresolved_only else ""
            rows = connection.execute(
                f"""SELECT * FROM live_intents WHERE user_name = ?{where}
                    ORDER BY prepared_at, operation_id""",
                (user,),
            ).fetchall()
            return [self._intent_dict(row) for row in rows]

        return self._read(load)

    def create_test_operation(
        self,
        user_name: str,
        *,
        operation_id: str,
        parent_id: str | None,
        direction: str,
        route: str,
        descriptor: Mapping[str, Any],
        requested_amount: str | Decimal,
        now: int | None = None,
    ) -> dict[str, Any]:
        """Persist an isolated manual transfer before any external submission."""

        user = _validate_identifier(user_name, "user_name")
        operation = self._validate_operation_id(operation_id, "operation_id")
        parent = self._validate_operation_id(parent_id, "parent_id") if parent_id is not None else None
        normalized_route = self._validate_operation_id(route, "route")
        if direction not in {"forward", "back"}:
            raise ValueError("direction must be forward or back")
        if (direction == "forward" and parent is not None) or (direction == "back" and parent is None):
            raise ValueError("test operation parent does not match its direction")
        amount = _decimal(requested_amount, "requested_amount", Decimal("0"))
        if amount <= 0:
            raise ValueError("requested_amount must be greater than zero")
        descriptor_value = dict(descriptor)
        if (
            descriptor_value.get("operation_id") != operation
            or descriptor_value.get("route") != normalized_route
            or _decimal(descriptor_value.get("amount"), "descriptor amount", Decimal("0")) != amount
        ):
            raise ValueError("descriptor does not match the test operation")
        descriptor_json = _canonical_json(descriptor_value)
        timestamp = int(time.time()) if now is None else now
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("now must be a non-negative integer Unix timestamp")

        def create(connection: sqlite3.Connection) -> dict[str, Any]:
            """Insert one test row without reading or changing sweep financial state."""

            if direction == "back":
                forward = self._test_operation_row(connection, str(parent))
                if (
                    forward["user_name"] != user
                    or forward["direction"] != "forward"
                    or forward["state"] != "confirmed"
                ):
                    raise ValueError("test transfer back requires a confirmed forward operation")
                existing = connection.execute(
                    "SELECT 1 FROM test_operations WHERE parent_id = ? AND direction = 'back'",
                    (parent,),
                ).fetchone()
                if existing is not None:
                    raise ValueError("test transfer has already been sent back")
            connection.execute(
                """INSERT INTO test_operations (
                    operation_id, user_name, parent_id, direction, route, descriptor_json, state,
                    requested_amount, prepared_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'prepared', ?, ?, ?)""",
                (
                    operation,
                    user,
                    parent,
                    direction,
                    normalized_route,
                    descriptor_json,
                    _decimal_text(amount),
                    timestamp,
                    timestamp,
                ),
            )
            return self._test_operation_dict(self._test_operation_row(connection, operation))

        return self._write(create)

    def get_test_operation(self, operation_id: str) -> dict[str, Any]:
        """Return one isolated manual transfer operation."""

        operation = self._validate_operation_id(operation_id, "operation_id")
        return self._read(
            lambda connection: self._test_operation_dict(self._test_operation_row(connection, operation))
        )

    def list_test_operations(self, user_name: str) -> list[dict[str, Any]]:
        """List manual transfer operations without requiring a sweep policy."""

        user = _validate_identifier(user_name, "user_name")

        def load(connection: sqlite3.Connection) -> list[dict[str, Any]]:
            """Load one user's isolated test operations in creation order."""

            rows = connection.execute(
                """SELECT * FROM test_operations WHERE user_name = ?
                   ORDER BY prepared_at, operation_id""",
                (user,),
            ).fetchall()
            return [self._test_operation_dict(row) for row in rows]

        return self._read(load)

    def list_unresolved_test_operations(self) -> list[dict[str, Any]]:
        """List every test operation that may require recovery or restart blocking."""

        def load(connection: sqlite3.Connection) -> list[dict[str, Any]]:
            """Load unresolved rows independently of the current exchange-user catalog."""

            rows = connection.execute(
                """SELECT * FROM test_operations
                   WHERE state IN ('prepared', 'submitting', 'unknown')
                   ORDER BY prepared_at, operation_id"""
            ).fetchall()
            return [self._test_operation_dict(row) for row in rows]

        return self._read(load)

    def transition_test_operation(
        self,
        operation_id: str,
        *,
        submission: Mapping[str, Any],
        claim: bool = False,
        now: int | None = None,
    ) -> dict[str, Any]:
        """Mark one prepared test operation as submitting or update its result."""

        operation = self._validate_operation_id(operation_id, "operation_id")
        submission_json = _canonical_json(dict(submission))
        timestamp = int(time.time()) if now is None else now
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("now must be a non-negative integer Unix timestamp")

        def transition(connection: sqlite3.Connection) -> dict[str, Any]:
            """Persist submission metadata without touching policy or financial rows."""

            row = self._test_operation_row(connection, operation)
            if claim and row["state"] != "prepared":
                raise ValueError("test operation submission has already been claimed")
            if row["state"] not in {"prepared", "submitting"}:
                raise ValueError("test operation has already been submitted")
            submitted_at = row["submitted_at"] if row["submitted_at"] is not None else timestamp
            connection.execute(
                """UPDATE test_operations SET state = 'submitting', submission_json = ?,
                   updated_at = ?, submitted_at = ? WHERE operation_id = ?""",
                (submission_json, timestamp, submitted_at, operation),
            )
            return self._test_operation_dict(self._test_operation_row(connection, operation))

        return self._write(transition)

    def reconcile_test_operation(
        self,
        operation_id: str,
        reconciliation: Mapping[str, Any],
        *,
        now: int | None = None,
    ) -> dict[str, Any]:
        """Persist a test reconciliation without sweep accounting side effects."""

        operation = self._validate_operation_id(operation_id, "operation_id")
        if not isinstance(reconciliation, Mapping):
            raise ValueError("reconciliation must be a mapping")
        status = str(reconciliation.get("status") or "unknown").lower()
        target = status if status in {"confirmed", "failed"} else "unknown"
        timestamp = int(time.time()) if now is None else now
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("now must be a non-negative integer Unix timestamp")
        error_value = reconciliation.get("error")
        if isinstance(error_value, Mapping):
            error_json = _canonical_json(dict(error_value))
        elif target != "confirmed":
            error_json = _canonical_json({"reason": str(reconciliation.get("reason") or target)[:128]})
        else:
            error_json = None
        actual_amount: str | None = None
        if reconciliation.get("received_amount") is not None:
            actual = _decimal(reconciliation["received_amount"], "received_amount", Decimal("0"))
            if actual <= 0:
                raise ValueError("received_amount must be greater than zero")
            actual_amount = _decimal_text(actual)

        def reconcile(connection: sqlite3.Connection) -> dict[str, Any]:
            """Resolve one test row idempotently and independently of live state."""

            row = self._test_operation_row(connection, operation)
            if row["state"] in {"confirmed", "failed"}:
                return self._test_operation_dict(row)
            if row["state"] not in {"submitting", "unknown"}:
                raise ValueError("only submitted test operations can be reconciled")
            if actual_amount is not None and Decimal(actual_amount) > Decimal(row["requested_amount"]):
                raise ValueError("received_amount exceeds requested_amount")
            connection.execute(
                """UPDATE test_operations SET state = ?, actual_amount = COALESCE(?, actual_amount),
                   error_json = ?, updated_at = ?, resolved_at = ? WHERE operation_id = ?""",
                (
                    target,
                    actual_amount,
                    error_json,
                    timestamp,
                    timestamp if target in {"confirmed", "failed"} else None,
                    operation,
                ),
            )
            return self._test_operation_dict(self._test_operation_row(connection, operation))

        return self._write(reconcile)

    def transition_live_intent(
        self,
        operation_id: str,
        state: str,
        *,
        submission: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
        claim: bool = False,
        now: int | None = None,
    ) -> dict[str, Any]:
        """Persist a non-financial intent transition or submission result."""

        operation = self._validate_operation_id(operation_id, "operation_id")
        if state not in {"prepared", "submitting", "confirmed", "failed", "unknown"}:
            raise ValueError("invalid live intent state")
        submission_json = _canonical_json(dict(submission)) if submission is not None else None
        error_json = _canonical_json(dict(error)) if error is not None else None
        timestamp = int(time.time()) if now is None else now
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("now must be a non-negative integer Unix timestamp")

        def transition(connection: sqlite3.Connection) -> dict[str, Any]:
            """Apply only allowed state-machine edges."""

            row = self._intent_row(connection, operation)
            current = row["state"]
            if claim and current != "prepared":
                raise ValueError("live intent submission has already been claimed")
            allowed = {
                "prepared": {"submitting"},
                "submitting": {"submitting"},
                "unknown": {"unknown"},
                "confirmed": set(),
                "failed": set(),
            }
            if state != current and state not in allowed[current]:
                raise ValueError(f"invalid live intent transition: {current} -> {state}")
            submitted_at = row["submitted_at"]
            if state == "submitting" and submitted_at is None:
                submitted_at = timestamp
            resolved_at = timestamp if state in {"confirmed", "failed"} else row["resolved_at"]
            connection.execute(
                """UPDATE live_intents SET state = ?, submission_json = COALESCE(?, submission_json),
                   error_json = COALESCE(?, error_json), updated_at = ?, submitted_at = ?, resolved_at = ?
                   WHERE operation_id = ?""",
                (state, submission_json, error_json, timestamp, submitted_at, resolved_at, operation),
            )
            return self._intent_dict(self._intent_row(connection, operation))

        return self._write(transition)

    def reconcile_live_intent(
        self,
        operation_id: str,
        reconciliation: Mapping[str, Any],
        *,
        settle_financial: bool = True,
        accounting_amount: str | Decimal | None = None,
        now: int | None = None,
    ) -> dict[str, Any]:
        """Resolve reconciliation atomically with due and policy accounting."""

        operation = self._validate_operation_id(operation_id, "operation_id")
        if not isinstance(reconciliation, Mapping):
            raise ValueError("reconciliation must be a mapping")
        status = str(reconciliation.get("status") or "unknown").lower()
        target = status if status in {"confirmed", "failed"} else "unknown"
        error_value = reconciliation.get("error")
        if isinstance(error_value, Mapping):
            error_json = _canonical_json(error_value)
        elif target != "confirmed":
            error_json = _canonical_json({"reason": str(reconciliation.get("reason") or target)[:128]})
        else:
            error_json = None
        timestamp = int(time.time()) if now is None else now
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("now must be a non-negative integer Unix timestamp")

        def reconcile(connection: sqlite3.Connection) -> dict[str, Any]:
            """Apply one idempotent reconciliation result under the financial lock."""

            row = self._intent_row(connection, operation)
            if row["state"] in {"confirmed", "failed"}:
                return self._intent_dict(row)
            if row["state"] not in {"submitting", "unknown"}:
                raise ValueError("only submitted intents can be reconciled")
            user = row["user_name"]
            policy_row = self._policy_row(connection, user)
            policy = json.loads(policy_row["config_json"])
            if target == "confirmed" and not settle_financial and not (
                row["route"] == "vault_to_main_perps"
                and row["parent_id"] is not None
                and row["operation_id"] != row["parent_id"]
            ):
                raise ValueError("only a forwarding Vault leg may defer financial settlement")
            if target == "confirmed" and settle_financial:
                reserved = Decimal(row["reserved_amount"])
                amount = reserved if accounting_amount is None else _decimal(
                    accounting_amount, "accounting_amount", Decimal("0")
                )
                if amount <= 0 or amount > reserved:
                    raise ValueError("accounting_amount must be positive and no greater than the reservation")
                if row["route"] != "vault_to_main_perps" and amount != reserved:
                    raise ValueError("non-Vault accounting must settle the full reservation")
                live = connection.execute("SELECT * FROM live_state WHERE user_name = ?", (user,)).fetchone()
                day = datetime.fromtimestamp(timestamp, tz=timezone.utc).date().isoformat()
                daily = Decimal(live["daily_total"]) if live["daily_date"] == day else Decimal("0")
                connection.execute(
                    """UPDATE live_state SET sweep_due = ?, confirmed_total = ?, daily_date = ?,
                       daily_total = ?, last_decision = 'confirmed' WHERE user_name = ?""",
                    (
                        _decimal_text(max(Decimal("0"), Decimal(live["sweep_due"]) - amount)),
                        _decimal_text(Decimal(live["confirmed_total"]) + amount),
                        day,
                        _decimal_text(daily + amount),
                        user,
                    ),
                )
            failed_vault_forward = row["route"] == "main_perps_to_spot" and target == "failed"
            if target == "unknown" or failed_vault_forward:
                policy["operating_mode"] = "paused_unknown"
                connection.execute(
                    "UPDATE policies SET config_json = ?, updated_at = ? WHERE user_name = ?",
                    (_canonical_json(policy), timestamp, user),
                )
            elif (
                target in {"confirmed", "failed"}
                and not failed_vault_forward
                and policy["operating_mode"] == "paused_unknown"
            ):
                policy["operating_mode"] = "live"
                connection.execute(
                    "UPDATE policies SET config_json = ?, updated_at = ? WHERE user_name = ?",
                    (_canonical_json(policy), timestamp, user),
                )
            connection.execute(
                """UPDATE live_intents SET state = ?, error_json = COALESCE(?, error_json),
                   updated_at = ?, resolved_at = ? WHERE operation_id = ?""",
                (
                    target,
                    error_json,
                    timestamp,
                    timestamp if target in {"confirmed", "failed"} else None,
                    operation,
                ),
            )
            return self._intent_dict(self._intent_row(connection, operation))

        return self._write(reconcile)

    def list_simulation_journal(self, user_name: str, limit: int = 100) -> list[dict[str, Any]]:
        """Return recent secret-free dry decisions without exposing internal row IDs."""
        user = _validate_identifier(user_name, "user_name")
        if type(limit) is not int or limit < 1 or limit > 1_000:
            raise ValueError("limit must be an integer between 1 and 1000")

        def load(connection: sqlite3.Connection) -> list[dict[str, Any]]:
            """Load and normalize recent simulation journal rows."""
            rows = connection.execute(
                "SELECT * FROM simulation_journal WHERE user_name = ? ORDER BY sequence DESC LIMIT ?", (user, limit)
            ).fetchall()
            return [
                {
                    "user_name": row["user_name"],
                    "generation": row["generation"],
                    "created_at": row["created_at"],
                    "net_pnl": row["net_pnl"],
                    "high_watermark": row["high_watermark"],
                    "due_before": row["due_before"],
                    "due_after": row["due_after"],
                    "amount": row["amount"],
                    "safety_reserve": row["safety_reserve"],
                    "effective_cap": row["effective_cap"],
                    "reason": row["reason"],
                }
                for row in rows
            ]

        return self._read(load)

    def set_scheduler_hints(
        self,
        user_name: str,
        *,
        state_kind: str = "simulation",
        next_run_at: int | None = None,
        last_event_at: int | None = None,
        last_successful_scan_at: int | None = None,
    ) -> dict[str, Any]:
        """Persist scheduler hint timestamps without evaluating or creating an intent."""
        user = _validate_identifier(user_name, "user_name")
        if state_kind not in _STATE_TABLES:
            raise ValueError("state_kind must be simulation or live")
        values = {
            "next_run_at": next_run_at,
            "last_event_at": last_event_at,
            "last_successful_scan_at": last_successful_scan_at,
        }
        provided = {field: value for field, value in values.items() if value is not None}
        if not provided:
            raise ValueError("at least one scheduler hint is required")
        for field, value in provided.items():
            if type(value) is not int or value < 0:
                raise ValueError(f"{field} must be a non-negative integer Unix timestamp")
        table = _STATE_TABLES[state_kind]

        def update(connection: sqlite3.Connection) -> dict[str, Any]:
            """Update only allowlisted scheduler columns in one state row."""
            self._policy_row(connection, user)
            assignments = ", ".join(f"{field} = ?" for field in provided)
            connection.execute(f"UPDATE {table} SET {assignments} WHERE user_name = ?", (*provided.values(), user))
            row = connection.execute(f"SELECT * FROM {table} WHERE user_name = ?", (user,)).fetchone()
            return self._state_dict(row, state_kind)

        return self._write(update)

    def database_settings(self) -> dict[str, Any]:
        """Return JSON-safe durability settings for diagnostics and tests."""
        def settings(connection: sqlite3.Connection) -> dict[str, Any]:
            """Read the active SQLite settings from one connection."""
            return {
                "schema_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
                "journal_mode": str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
                "synchronous": int(connection.execute("PRAGMA synchronous").fetchone()[0]),
                "busy_timeout_ms": int(connection.execute("PRAGMA busy_timeout").fetchone()[0]),
            }

        return self._read(settings)
