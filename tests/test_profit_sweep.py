"""Offline tests for profit-sweep policy, Dry state, and durable Live intents."""

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import json
import os
from pathlib import Path
import sqlite3
import stat

import pytest

from profit_sweep import ProfitSweepStore, _policy_fingerprint, calculate_sweep, default_policy


def _store(tmp_path: Path, policy: dict | None = None, *, baseline: str = "0") -> ProfitSweepStore:
    """Create one isolated store and dry policy for a test."""
    store = ProfitSweepStore(tmp_path / "profit-sweep" / "state.sqlite3")
    values = {"operating_mode": "dry", **(policy or {})}
    store.create_policy("alice", "hyperliquid", values, baseline_net_pnl=baseline)
    return store


def _assert_no_live_artifacts(value: object) -> None:
    """Assert recursively that a dry response exposes no live-allocation fields."""
    forbidden = ("transfer_id", "clientoid", "nonce", "signature", "intent", "signed_action")
    if isinstance(value, dict):
        for key, child in value.items():
            assert not any(token in key.lower() for token in forbidden)
            _assert_no_live_artifacts(child)
    elif isinstance(value, list):
        for child in value:
            _assert_no_live_artifacts(child)


def test_private_database_permissions_settings_and_decimal_storage(tmp_path: Path) -> None:
    """The dedicated database is private, durable, versioned, and stores money as TEXT."""
    store = _store(tmp_path)
    settings = store.database_settings()

    assert stat.S_IMODE(store.db_path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(store.db_path.stat().st_mode) == 0o600
    assert settings == {
        "schema_version": 5,
        "journal_mode": "wal",
        "synchronous": 2,
        "busy_timeout_ms": 5_000,
    }
    assert store.get_policy("alice")["live_state"]["active_baseline_mode"] == "legacy_unknown"

    store.upsert_ledger_event(
        user_name="alice",
        exchange="hyperliquid",
        event_time_ms=1,
        event_hash="hash-1",
        event_type="fill",
        asset="USDT",
        realized_trade_pnl="12.345",
        fees="0.1",
        payload={"source": "fixture"},
    )
    with sqlite3.connect(store.db_path) as connection:
        types = connection.execute(
            "SELECT typeof(realized_trade_pnl), typeof(fees) FROM ledger_events"
        ).fetchone()
    assert types == ("text", "text")


def test_symlink_database_is_rejected(tmp_path: Path) -> None:
    """A configured database symlink is rejected before SQLite follows it."""
    target = tmp_path / "target.sqlite3"
    target.touch()
    link = tmp_path / "linked.sqlite3"
    link.symlink_to(target)

    with pytest.raises(ValueError, match="symlink"):
        ProfitSweepStore(link)


def test_schema_version_mismatch_is_rejected(tmp_path: Path) -> None:
    """A future or otherwise unsupported schema version fails closed."""
    db_path = tmp_path / "versioned" / "state.sqlite3"
    store = ProfitSweepStore(db_path)
    with sqlite3.connect(store.db_path) as connection:
        connection.execute("PRAGMA user_version=99")

    with pytest.raises(RuntimeError, match="unsupported"):
        ProfitSweepStore(db_path)


def test_policy_defaults_crud_persistence_and_scheduler_hints(tmp_path: Path) -> None:
    """Policy state and scheduling hints survive a new store instance."""
    db_path = tmp_path / "persistent" / "state.sqlite3"
    store = ProfitSweepStore(db_path)
    created = store.create_policy("alice", "binance", {"operating_mode": "dry"}, baseline_net_pnl="40")
    hint = store.set_scheduler_hints(
        "alice", next_run_at=100, last_event_at=90, last_successful_scan_at=80
    )
    reopened = ProfitSweepStore(db_path)
    loaded = reopened.get_policy("alice")

    assert set(loaded["policy"]) == set(default_policy())
    assert created["simulation_state"]["baseline_pnl"] == "40"
    assert loaded["simulation_state"]["next_run_at"] == hint["next_run_at"] == 100
    assert loaded["live_state"]["confirmed_total"] == "0"
    assert json.loads(json.dumps(loaded)) == loaded
    assert reopened.delete_policy("alice") is True
    assert reopened.delete_policy("alice") is False


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"trigger_percent": "-1"}, "trigger_percent"),
        ({"sweep_percent": "100.01"}, "sweep_percent"),
        ({"transfer_rounding_step": "-0.1"}, "transfer_rounding_step"),
        ({"schedule_jitter_percent": "51"}, "schedule_jitter_percent"),
        ({"daily_transfer_limit_enabled": True, "daily_transfer_limit": "0"}, "daily_transfer_limit"),
        ({"periodic_interval": 0}, "periodic_interval"),
        ({"asset": "usdt"}, "asset"),
        ({"reference_capital": 1000}, "decimal string"),
        ({"unknown": "value"}, "unknown policy fields"),
    ],
)
def test_policy_validation_rejects_unsafe_or_malformed_values(
    tmp_path: Path, changes: dict, message: str
) -> None:
    """Every policy field remains strictly typed and bounded."""
    store = ProfitSweepStore(tmp_path / "validation" / "state.sqlite3")

    with pytest.raises(ValueError, match=message):
        store.create_policy("alice", "binance", changes)


def test_trigger_zero_sweep_full_and_twenty_five_batching(tmp_path: Path) -> None:
    """Keep-capital mode accumulates and simulates exact 25-unit batches."""
    store = _store(
        tmp_path,
        {
            "reference_capital": "1000",
            "trigger_percent": "0",
            "sweep_percent": "100",
            "minimum_transfer_amount": "25",
        },
    )

    first = store.evaluate_dry("alice", cumulative_net_pnl="10", max_transferable="1000", now=1)
    second = store.evaluate_dry("alice", cumulative_net_pnl="25", max_transferable="1000", now=2)
    third = store.evaluate_dry("alice", cumulative_net_pnl="49", max_transferable="1000", now=3)
    fourth = store.evaluate_dry("alice", cumulative_net_pnl="50", max_transferable="1000", now=4)

    assert [item["amount"] for item in (first, second, third, fourth)] == ["0", "25", "0", "25"]
    assert fourth["high_watermark"] == "50"
    assert fourth["sweep_due_after_simulation"] == "0"
    assert fourth["simulated_total"] == "50"
    assert fourth["confirmed_total"] == "0"


def test_transfer_rounding_step_rounds_down_and_retains_due(tmp_path: Path) -> None:
    """Rounding never over-transfers and preserves the fractional remainder for later."""

    store = _store(
        tmp_path,
        {
            "trigger_percent": "0",
            "sweep_percent": "100",
            "minimum_transfer_amount": "0",
            "transfer_rounding_step": "1",
        },
    )

    first = store.evaluate_dry("alice", cumulative_net_pnl="2.1856825", max_transferable="100", now=1)
    second = store.evaluate_dry("alice", cumulative_net_pnl="2.1856825", max_transferable="100", now=2)
    tenths = calculate_sweep(
        {"trigger_percent": "0", "sweep_percent": "100", "minimum_transfer_amount": "0", "transfer_rounding_step": "0.1"},
        net_pnl="2.1856825",
        high_watermark="0",
        sweep_due="0",
        max_transferable="100",
    )

    assert first["amount"] == "2"
    assert first["sweep_due_after_simulation"] == "0.1856825"
    assert second["amount"] == "0"
    assert second["sweep_due_after_simulation"] == "0.1856825"
    assert second["reason"] == "below_rounding_step"
    assert tenths["amount"] == "2.1"


def test_legacy_policy_update_adds_disabled_rounding_default(tmp_path: Path) -> None:
    """Policies persisted before rounding existed remain editable without migration."""

    store = _store(tmp_path)
    legacy = store.get_policy("alice")["policy"]
    legacy.pop("transfer_rounding_step")
    with sqlite3.connect(store.db_path) as connection:
        connection.execute(
            "UPDATE policies SET config_json = ? WHERE user_name = 'alice'",
            (json.dumps(legacy, sort_keys=True, separators=(",", ":")),),
        )

    updated = store.update_policy("alice", {"quiet_period": 10})

    assert updated["policy"]["transfer_rounding_step"] == "0"


def test_loss_carryforward_requires_a_new_high_watermark(tmp_path: Path) -> None:
    """A loss and recovery to the old peak create no duplicate sweep."""
    store = _store(
        tmp_path,
        {
            "trigger_percent": "0",
            "sweep_percent": "100",
            "minimum_transfer_amount": "25",
        },
    )
    amounts = []
    for timestamp, pnl in enumerate(("25", "50", "10", "50", "75"), start=1):
        result = store.evaluate_dry(
            "alice", cumulative_net_pnl=pnl, max_transferable="1000", now=timestamp
        )
        amounts.append(result["amount"])

    assert amounts == ["25", "25", "0", "0", "25"]
    assert store.get_policy("alice")["simulation_state"]["high_watermark"] == "75"


def test_existing_due_waits_when_pnl_falls_below_trigger(tmp_path: Path) -> None:
    """A capped outstanding due cannot bypass the current positive trigger gate."""
    store = _store(
        tmp_path,
        {
            "reference_capital": "1000",
            "trigger_percent": "10",
            "sweep_percent": "100",
            "minimum_transfer_amount": "1",
            "single_transfer_limit_enabled": True,
            "single_transfer_limit": "25",
        },
    )
    first = store.evaluate_dry("alice", cumulative_net_pnl="100", max_transferable="1000", now=1)
    loss = store.evaluate_dry("alice", cumulative_net_pnl="50", max_transferable="1000", now=2)

    assert first["amount"] == "25"
    assert first["sweep_due_after_simulation"] == "75"
    assert loss["amount"] == "0"
    assert loss["sweep_due_after_simulation"] == "75"
    assert loss["reason"] == "trigger_not_reached"


def test_trigger_zero_existing_due_waits_for_high_watermark_recovery(tmp_path: Path) -> None:
    """Trigger zero must not pay capped old entitlement while PnL is below its peak."""
    store = _store(
        tmp_path,
        {
            "trigger_percent": "0",
            "sweep_percent": "100",
            "minimum_transfer_amount": "1",
            "single_transfer_limit_enabled": True,
            "single_transfer_limit": "25",
        },
    )

    peak = store.evaluate_dry("alice", cumulative_net_pnl="100", max_transferable="1000", now=1)
    loss = store.evaluate_dry("alice", cumulative_net_pnl="50", max_transferable="1000", now=2)
    recovery = store.evaluate_dry("alice", cumulative_net_pnl="100", max_transferable="1000", now=3)

    assert peak["amount"] == "25"
    assert peak["sweep_due_after_simulation"] == "75"
    assert loss["amount"] == "0"
    assert loss["reason"] == "below_high_watermark_recovery"
    assert recovery["amount"] == "25"


def test_transfer_caps_reserve_single_daily_and_minimum() -> None:
    """The effective decision uses every configured financial cap and minimum."""
    policy = {
        **default_policy(),
        "reference_capital": "1000",
        "trigger_percent": "0",
        "sweep_percent": "100",
        "minimum_transfer_amount": "25",
        "safety_reserve_mode": "max_of_both",
        "safety_reserve_amount": "100",
        "safety_reserve_percent": "5",
        "single_transfer_limit_enabled": True,
        "single_transfer_limit": "80",
        "daily_transfer_limit_enabled": True,
        "daily_transfer_limit": "60",
    }
    capped = calculate_sweep(
        policy,
        net_pnl="200",
        high_watermark="0",
        sweep_due="0",
        max_transferable="500",
        transferred_today="20",
    )
    below_minimum = calculate_sweep(
        policy,
        net_pnl="200",
        high_watermark="0",
        sweep_due="0",
        max_transferable="120",
        transferred_today="20",
    )

    assert capped["safety_reserve"] == "100"
    assert capped["effective_cap"] == "40"
    assert capped["amount"] == "40"
    assert below_minimum["effective_cap"] == "20"
    assert below_minimum["amount"] == "0"


def test_minimum_override_and_negative_fee_rebate_are_supported(tmp_path: Path) -> None:
    """Vault-specific minimums and negative fee rebates retain exact Decimal semantics."""
    policy = {
        **default_policy(),
        "trigger_percent": "0",
        "sweep_percent": "100",
        "simulation_minimum_transfer_amount": "1",
    }
    decision = calculate_sweep(
        policy,
        net_pnl="49",
        high_watermark="0",
        sweep_due="0",
        max_transferable="100",
        minimum_transfer_override="50",
    )
    assert decision["amount"] == "0"

    store = _store(tmp_path)
    store.upsert_ledger_event(
        user_name="alice",
        exchange="hyperliquid",
        event_time_ms=1,
        event_hash="rebate-event",
        event_type="fill",
        asset="USDC",
        realized_trade_pnl="1",
        fees="-0.2",
    )
    assert store.ledger_net_pnl("alice", "hyperliquid", "USDC") == "1.2"


def test_baseline_modes_and_generation_reset_rules(tmp_path: Path) -> None:
    """From-enable offsets PnL while lifetime starts at zero and resets are explicit."""
    store = _store(
        tmp_path,
        {"trigger_percent": "0", "sweep_percent": "100", "minimum_transfer_amount": "0"},
        baseline="100",
    )
    evaluated = store.evaluate_dry("alice", cumulative_net_pnl="125", max_transferable="1000", now=1)
    non_baseline = store.update_policy("alice", {"quiet_period": 10})
    reset = store.update_policy(
        "alice", {"reference_capital": "2000"}, baseline_net_pnl="125"
    )
    lifetime = store.update_policy("alice", {"baseline_mode": "lifetime"})

    assert evaluated["net_pnl"] == "25"
    assert non_baseline["generation"] == 1
    assert non_baseline["simulation_state"]["high_watermark"] == "25"
    assert reset["generation"] == 2
    assert reset["simulation_state"]["high_watermark"] == "0"
    assert reset["simulation_state"]["baseline_pnl"] == "125"
    assert lifetime["generation"] == 3
    assert lifetime["simulation_state"]["baseline_pnl"] == "0"


def test_full_unchanged_policy_save_does_not_reset_accounting_generation(tmp_path: Path) -> None:
    """Saving a complete unchanged form must preserve HWM, due, and both state generations."""

    store = _store(
        tmp_path,
        {"trigger_percent": "0", "sweep_percent": "50", "minimum_transfer_amount": "0"},
        baseline="100",
    )
    store.evaluate_dry("alice", cumulative_net_pnl="125", max_transferable="1000", now=1)
    before = store.get_policy("alice")

    saved = store.update_policy("alice", before["policy"])

    assert saved["generation"] == before["generation"]
    assert saved["simulation_state"] == before["simulation_state"]
    assert saved["live_state"] == before["live_state"]


def test_active_live_can_rebaseline_to_include_dry_before_any_transfer(tmp_path: Path) -> None:
    """Switching Fresh to Include Dry Period retroactively restores Dry-period entitlement."""

    store = _store(
        tmp_path,
        {"trigger_percent": "0", "sweep_percent": "50", "minimum_transfer_amount": "0"},
        baseline="100",
    )
    store.evaluate_dry("alice", cumulative_net_pnl="125", max_transferable="1000", now=1)
    activated = store.activate_live("alice", "125", baseline_mode="fresh")

    assert activated["live_state"]["baseline_pnl"] == "125"
    assert activated["live_state"]["active_baseline_mode"] == "fresh"
    current = store.get_policy("alice")
    changes = {**current["policy"], "live_activation_baseline_mode": "include_dry_period"}
    rebaselined = store.rebaseline_live(
        "alice",
        "125",
        "include_dry_period",
        changes,
        expected_policy_fingerprint=_policy_fingerprint(current["policy"]),
    )
    preview = store.evaluate_live(
        "alice", cumulative_net_pnl="125", max_transferable="1000", now=2, commit=False
    )

    assert rebaselined["live_state"]["baseline_pnl"] == "100"
    assert rebaselined["live_state"]["active_baseline_mode"] == "include_dry_period"
    assert preview["net_pnl"] == "25"
    assert preview["amount"] == "12.5"


def test_retroactive_live_rebaseline_is_blocked_after_confirmed_transfer(tmp_path: Path) -> None:
    """Previously moved funds make retroactive entitlement unsafe to recompute."""

    store = _live_store(tmp_path)
    _create_intent(store, "already-confirmed", amount="25")
    store.transition_live_intent(
        "already-confirmed",
        "submitting",
        submission={"status": "submitted"},
        claim=True,
        now=2,
    )
    store.reconcile_live_intent("already-confirmed", {"status": "confirmed"}, now=3)
    current = store.get_policy("alice")

    with pytest.raises(ValueError, match="confirmed Live transfers"):
        store.rebaseline_live(
            "alice",
            "100",
            "include_dry_period",
            {**current["policy"], "live_activation_baseline_mode": "include_dry_period"},
            expected_policy_fingerprint=_policy_fingerprint(current["policy"]),
        )


def test_legacy_empty_live_state_recovers_previous_dry_baseline_on_explicit_switch(
    tmp_path: Path,
) -> None:
    """An explicit Include Dry switch repairs the known v2.0 full-save reset pattern."""

    store = _store(
        tmp_path,
        {"trigger_percent": "0", "sweep_percent": "50", "minimum_transfer_amount": "0"},
        baseline="100",
    )
    store.evaluate_dry("alice", cumulative_net_pnl="125", max_transferable="1000", now=1)
    store.update_policy("alice", {"reference_capital": "2000"})
    store.activate_live("alice", "125", baseline_mode="fresh")
    with sqlite3.connect(store.db_path) as connection:
        connection.execute(
            "UPDATE live_state SET active_baseline_mode = 'legacy_unknown' WHERE user_name = 'alice'"
        )
    current = store.get_policy("alice")

    repaired = store.rebaseline_live(
        "alice",
        "125",
        "include_dry_period",
        {**current["policy"], "live_activation_baseline_mode": "include_dry_period"},
        expected_policy_fingerprint=_policy_fingerprint(current["policy"]),
        recover_legacy_dry_generation=True,
    )

    assert repaired["live_state"]["baseline_pnl"] == "100"


def test_legacy_dry_recovery_never_creates_negative_lifetime_baseline(tmp_path: Path) -> None:
    """Lifetime accounting remains anchored at zero even when a legacy journal exists."""

    store = _store(
        tmp_path,
        {
            "baseline_mode": "lifetime",
            "trigger_percent": "0",
            "sweep_percent": "50",
            "minimum_transfer_amount": "0",
        },
    )
    store.evaluate_dry("alice", cumulative_net_pnl="125", max_transferable="1000", now=1)
    store.update_policy("alice", {"reference_capital": "2000"})
    store.activate_live("alice", "125", baseline_mode="fresh")
    with sqlite3.connect(store.db_path) as connection:
        connection.execute(
            "UPDATE live_state SET active_baseline_mode = 'legacy_unknown' WHERE user_name = 'alice'"
        )
    current = store.get_policy("alice")

    with pytest.raises(ValueError, match="cannot be recovered"):
        store.rebaseline_live(
            "alice",
            "125",
            "include_dry_period",
            {**current["policy"], "live_activation_baseline_mode": "include_dry_period"},
            expected_policy_fingerprint=_policy_fingerprint(current["policy"]),
            recover_legacy_dry_generation=True,
        )


def test_policy_asset_change_starts_new_generation_from_supplied_baseline(tmp_path: Path) -> None:
    """Changing settlement asset resets accounting state against the supplied baseline."""

    store = _store(
        tmp_path,
        {"trigger_percent": "0", "sweep_percent": "100", "minimum_transfer_amount": "0"},
        baseline="100",
    )
    store.evaluate_dry("alice", cumulative_net_pnl="125", max_transferable="1000", now=1)

    changed = store.update_policy("alice", {"asset": "USDC"}, baseline_net_pnl="200")

    assert changed["policy"]["asset"] == "USDC"
    assert changed["generation"] == 2
    assert changed["simulation_state"]["generation"] == 2
    assert changed["simulation_state"]["baseline_pnl"] == "200"
    assert changed["simulation_state"]["high_watermark"] == "0"
    assert changed["live_state"]["generation"] == 2
    assert changed["live_state"]["baseline_pnl"] == "200"


def test_dry_preview_and_commit_never_touch_live_state_or_allocate_artifacts(tmp_path: Path) -> None:
    """Read-only preview and dry commit remain isolated from all live state."""
    store = _store(
        tmp_path,
        {"trigger_percent": "0", "sweep_percent": "100", "minimum_transfer_amount": "1"},
    )
    before = store.get_policy("alice")
    preview = store.evaluate_dry(
        "alice", cumulative_net_pnl="10", max_transferable="100", now=1, commit=False
    )
    after_preview = store.get_policy("alice")
    committed = store.evaluate_dry(
        "alice", cumulative_net_pnl="10", max_transferable="100", now=2
    )
    after_commit = store.get_policy("alice")

    assert before == after_preview
    assert preview["committed"] is False
    assert after_commit["simulation_state"]["simulated_total"] == "10"
    assert after_commit["live_state"] == before["live_state"]
    _assert_no_live_artifacts(preview)
    _assert_no_live_artifacts(committed)
    _assert_no_live_artifacts(store.list_simulation_journal("alice"))


def test_immutable_ledger_dedupe_and_decimal_pnl(tmp_path: Path) -> None:
    """Composite duplicates are idempotent, immutable, and summed exactly."""
    store = _store(tmp_path)
    event = {
        "user_name": "alice",
        "exchange": "hyperliquid",
        "event_time_ms": 1000,
        "event_hash": "hash-a",
        "event_type": "fill",
        "asset": "USDT",
        "realized_trade_pnl": "10.2",
        "funding": "0.3",
        "fees": "0.1",
        "exchange_corrections": "-0.4",
        "payload": {"trade": "one", "sequence": 1},
    }
    first = store.upsert_ledger_event(**event)
    duplicate = store.upsert_ledger_event(**event)

    assert first["inserted"] is True
    assert duplicate["inserted"] is False
    assert len(store.list_ledger_events("alice", "hyperliquid")) == 1
    assert store.ledger_net_pnl("alice", "hyperliquid", "USDT") == "10"
    with pytest.raises(ValueError, match="immutable"):
        store.upsert_ledger_event(**{**event, "fees": "0.2"})
    with pytest.raises(ValueError, match="immutable"):
        store.upsert_ledger_event(**{**event, "asset": "USDC"})
    with pytest.raises(ValueError, match="immutable"):
        store.upsert_ledger_event(**{**event, "payload": {"trade": "changed", "sequence": 1}})


def test_ledger_pnl_is_isolated_by_settlement_asset(tmp_path: Path) -> None:
    """Policy-bound and explicit PnL reads never combine different settlement assets."""

    store = _store(tmp_path)
    common = {
        "user_name": "alice",
        "exchange": "hyperliquid",
        "event_type": "fill",
        "fees": "0",
    }
    store.upsert_ledger_event(
        **common,
        event_time_ms=1,
        event_hash="usdt-event",
        asset="USDT",
        realized_trade_pnl="10",
    )
    store.upsert_ledger_event(
        **common,
        event_time_ms=2,
        event_hash="usdc-event",
        asset="USDC",
        realized_trade_pnl="20",
    )

    assert len(store.list_ledger_events("alice", "hyperliquid")) == 2
    assert len(store.list_ledger_events("alice", "hyperliquid", "USDC")) == 1
    assert store.ledger_net_pnl("alice", "hyperliquid") == "10"
    assert store.ledger_net_pnl("alice", "hyperliquid", "USDT") == "10"
    assert store.ledger_net_pnl("alice", "hyperliquid", "USDC") == "20"
    store.update_policy("alice", {"asset": "USDC"}, baseline_net_pnl="0")
    assert store.ledger_net_pnl("alice", "hyperliquid") == "20"


def test_concurrent_event_writes_are_serialized_and_persistent(tmp_path: Path) -> None:
    """Independent store instances serialize concurrent ledger writes without loss."""
    db_path = tmp_path / "concurrent" / "state.sqlite3"
    store = ProfitSweepStore(db_path)
    store.create_policy("alice", "hyperliquid", {"operating_mode": "dry"})

    def insert(index: int) -> bool:
        """Insert one unique event through an independent connection owner."""
        worker = ProfitSweepStore(db_path)
        return worker.upsert_ledger_event(
            user_name="alice",
            exchange="hyperliquid",
            event_time_ms=index,
            event_hash=f"hash-{index}",
            event_type="fill",
            asset="USDT",
            realized_trade_pnl="0.1",
            payload={"index": index},
        )["inserted"]

    with ThreadPoolExecutor(max_workers=8) as executor:
        inserted = list(executor.map(insert, range(32)))

    reopened = ProfitSweepStore(db_path)
    assert all(inserted)
    assert len(reopened.list_ledger_events("alice", "hyperliquid")) == 32
    assert Decimal(reopened.ledger_net_pnl("alice", "hyperliquid", "USDT")) == Decimal("3.2")


def test_json_safe_contract_and_owner_only_sidecars(tmp_path: Path) -> None:
    """All public responses serialize as JSON and any present SQLite sidecars are private."""
    store = _store(tmp_path)
    response = store.evaluate_dry(
        "alice", cumulative_net_pnl="0", max_transferable="0", now=1
    )

    assert json.loads(json.dumps(response)) == response
    for suffix in ("-wal", "-shm", ".lock"):
        path = Path(f"{store.db_path}{suffix}")
        if path.exists():
            assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_disappearing_sqlite_sidecar_does_not_break_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SQLite may remove its SHM sidecar between existence and chmod checks."""

    store = _store(tmp_path)
    shm_path = Path(f"{store.db_path}-shm")
    shm_path.touch(mode=0o600)
    original_chmod = os.chmod
    raised = False

    def disappearing_chmod(path: Path, mode: int) -> None:
        """Simulate SQLite deleting the transient SHM file before chmod."""

        nonlocal raised
        if Path(path) == shm_path and not raised:
            raised = True
            shm_path.unlink(missing_ok=True)
            raise FileNotFoundError(shm_path)
        original_chmod(path, mode)

    monkeypatch.setattr("profit_sweep.os.chmod", disappearing_chmod)

    assert store.get_policy("alice")["user_name"] == "alice"
    assert raised is True


def _live_store(tmp_path: Path) -> ProfitSweepStore:
    """Create one isolated Live policy with exactly 100 units due."""

    store = ProfitSweepStore(tmp_path / "live" / "state.sqlite3")
    store.create_policy(
        "alice",
        "binance",
        {
            "operating_mode": "dry",
            "baseline_mode": "lifetime",
            "reference_capital": "1000",
            "trigger_percent": "0",
            "sweep_percent": "100",
            "live_minimum_transfer_amount": "1",
        },
    )
    store.activate_live("alice", "0", baseline_mode="include_dry_period")
    decision = store.evaluate_live(
        "alice", cumulative_net_pnl="100", max_transferable="1000", now=1
    )
    assert decision["amount"] == "100"
    return store


def _create_intent(
    store: ProfitSweepStore,
    operation_id: str,
    *,
    amount: str = "25",
) -> dict:
    """Create a minimal persisted descriptor for store state-machine tests."""

    return store.create_live_intent(
        "alice",
        operation_id=operation_id,
        parent_id=None,
        leg=1,
        route="umfuture_to_funding",
        descriptor={
            "operation_id": operation_id,
            "route": "umfuture_to_funding",
            "amount": amount,
        },
        reserved_amount=amount,
        now=2,
    )


def _install_v3_ledger(db_path: Path, events: list[tuple[object, ...]]) -> None:
    """Replace the current ledger with a version-three table and supplied rows."""

    with sqlite3.connect(db_path) as connection:
        connection.execute("DROP INDEX ledger_events_user_time")
        connection.execute("DROP TABLE ledger_events")
        connection.execute(
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
                PRIMARY KEY (user_name, exchange, event_time_ms, event_hash, event_type, payload_hash)
            )"""
        )
        connection.executemany(
            """INSERT INTO ledger_events (
                user_name, exchange, event_time_ms, event_hash, event_type, payload_hash,
                payload_json, asset, realized_trade_pnl, funding, fees,
                exchange_corrections, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            events,
        )
        connection.execute(
            "CREATE INDEX ledger_events_user_time ON ledger_events(user_name, exchange, event_time_ms)"
        )
        connection.execute("PRAGMA user_version=3")


def test_schema_v3_migration_collapses_only_exact_event_duplicates(tmp_path: Path) -> None:
    """Version three duplicate accounting rows collapse only when their values match."""

    db_path = tmp_path / "migration-v3-exact" / "state.sqlite3"
    store = _store(tmp_path / "migration-v3-source")
    db_path.parent.mkdir(parents=True)
    source = sqlite3.connect(store.db_path)
    target = sqlite3.connect(db_path)
    source.backup(target)
    source.close()
    target.close()
    identity = ("alice", "hyperliquid", 1, "same-event", "fill")
    immutable = ('{"trade":"one"}', "USDT", "10", "0", "0.1", "0")
    _install_v3_ledger(
        db_path,
        [
            (*identity, "old-payload-hash-a", *immutable, 10),
            (*identity, "old-payload-hash-b", *immutable, 11),
            (
                "alice", "hyperliquid", 2, "other-event", "funding", "other-payload-hash",
                "{}", "USDT", "0", "2", "0", "0", 12,
            ),
        ],
    )

    migrated = ProfitSweepStore(db_path)

    assert migrated.database_settings()["schema_version"] == 5
    assert len(migrated.list_ledger_events("alice", "hyperliquid")) == 2
    assert migrated.ledger_net_pnl("alice", "hyperliquid", "USDT") == "11.9"
    repeated = migrated.upsert_ledger_event(
        user_name="alice",
        exchange="hyperliquid",
        event_time_ms=1,
        event_hash="same-event",
        event_type="fill",
        asset="USDT",
        realized_trade_pnl="10",
        fees="0.1",
        payload={"trade": "one"},
    )
    assert repeated["inserted"] is False
    with sqlite3.connect(db_path) as connection:
        primary_key = [
            row[1]
            for row in connection.execute("PRAGMA table_info(ledger_events)")
            if row[5] > 0
        ]
    assert primary_key == ["user_name", "exchange", "event_time_ms", "event_hash", "event_type"]


def test_schema_v3_migration_rejects_conflicting_event_identities(tmp_path: Path) -> None:
    """A conflicting version-three identity aborts and preserves the old database."""

    db_path = tmp_path / "migration-v3-conflict" / "state.sqlite3"
    store = _store(tmp_path / "migration-v3-conflict-source")
    db_path.parent.mkdir(parents=True)
    source = sqlite3.connect(store.db_path)
    target = sqlite3.connect(db_path)
    source.backup(target)
    source.close()
    target.close()
    identity = ("alice", "hyperliquid", 1, "same-event", "fill")
    _install_v3_ledger(
        db_path,
        [
            (*identity, "payload-hash-a", '{"trade":"one"}', "USDT", "10", "0", "0", "0", 10),
            (*identity, "payload-hash-b", '{"trade":"changed"}', "USDT", "10", "0", "0", "0", 11),
        ],
    )

    with pytest.raises(RuntimeError, match="conflicting duplicate ledger event identity"):
        ProfitSweepStore(db_path)

    with sqlite3.connect(db_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        assert connection.execute("SELECT COUNT(*) FROM ledger_events").fetchone()[0] == 2


def test_schema_v1_migrates_to_v5_without_losing_policy_or_dry_state(tmp_path: Path) -> None:
    """Opening a version-one database adds later schemas without rewriting state."""

    db_path = tmp_path / "migration" / "state.sqlite3"
    store = ProfitSweepStore(db_path)
    store.create_policy(
        "alice",
        "hyperliquid",
        {
            "operating_mode": "dry",
            "baseline_mode": "lifetime",
            "trigger_percent": "0",
            "sweep_percent": "100",
            "simulation_minimum_transfer_amount": "1",
        },
    )
    store.evaluate_dry("alice", cumulative_net_pnl="12.5", max_transferable="100", now=1)
    before = store.get_policy("alice")
    with sqlite3.connect(db_path) as connection:
        connection.execute("DROP TABLE live_intents")
        connection.execute("DROP TABLE test_operations")
        connection.execute("PRAGMA user_version=1")

    migrated = ProfitSweepStore(db_path)

    assert migrated.database_settings()["schema_version"] == 5
    assert migrated.get_policy("alice") == before
    assert migrated.list_live_intents("alice") == []
    assert migrated.list_test_operations("alice") == []


def test_schema_v2_adds_isolated_test_operations(tmp_path: Path) -> None:
    """Opening a version-two database adds only the manual test-operation journal."""

    db_path = tmp_path / "migration-v2" / "state.sqlite3"
    store = _store(tmp_path / "migration-source")
    db_path.parent.mkdir(parents=True)
    source = sqlite3.connect(store.db_path)
    target = sqlite3.connect(db_path)
    source.backup(target)
    source.close()
    target.execute("DROP TABLE test_operations")
    target.execute("PRAGMA user_version=2")
    target.commit()
    target.close()

    migrated = ProfitSweepStore(db_path)

    assert migrated.database_settings()["schema_version"] == 5
    assert migrated.get_policy("alice")["simulation_state"]["sweep_due"] == "0"
    assert migrated.list_test_operations("alice") == []


def test_test_operations_never_change_sweep_financial_accounting(tmp_path: Path) -> None:
    """Forward, unknown, confirmed, and back test states remain outside Live accounting."""

    store = _live_store(tmp_path)
    before = store.get_policy("alice")["live_state"]
    forward = store.create_test_operation(
        "alice",
        operation_id="test-forward",
        parent_id=None,
        direction="forward",
        route="umfuture_to_funding",
        descriptor={
            "operation_id": "test-forward",
            "route": "umfuture_to_funding",
            "amount": "25",
        },
        requested_amount="25",
        now=2,
    )
    store.transition_test_operation(
        forward["operation_id"],
        submission={"status": "unknown", "submitted_at_ms": 2_000},
        now=2,
    )
    unknown = store.reconcile_test_operation(
        forward["operation_id"], {"status": "unknown", "reason": "timeout"}, now=3
    )
    confirmed = store.reconcile_test_operation(
        forward["operation_id"], {"status": "confirmed", "received_amount": "24.5"}, now=4
    )
    back = store.create_test_operation(
        "alice",
        operation_id="test-back",
        parent_id=forward["operation_id"],
        direction="back",
        route="funding_to_umfuture",
        descriptor={
            "operation_id": "test-back",
            "route": "funding_to_umfuture",
            "amount": "24.5",
        },
        requested_amount="24.5",
        now=5,
    )

    assert unknown["state"] == "unknown"
    assert confirmed["actual_amount"] == "24.5"
    assert back["state"] == "prepared"
    assert store.get_policy("alice")["live_state"] == before
    assert store.get_policy("alice")["policy"]["operating_mode"] == "live"
    with pytest.raises(ValueError, match="already been sent back"):
        store.create_test_operation(
            "alice",
            operation_id="test-back-duplicate",
            parent_id=forward["operation_id"],
            direction="back",
            route="funding_to_umfuture",
            descriptor={
                "operation_id": "test-back-duplicate",
                "route": "funding_to_umfuture",
                "amount": "24.5",
            },
            requested_amount="24.5",
            now=6,
        )


def test_live_failure_releases_reservation_without_reducing_due(tmp_path: Path) -> None:
    """A definitive failure is terminal and permits a replacement reservation."""

    store = _live_store(tmp_path)
    first = _create_intent(store, "operation-1")
    store.transition_live_intent(
        first["operation_id"],
        "submitting",
        submission={"status": "submitting", "submitted_at_ms": 2_000},
        now=2,
    )
    failed = store.reconcile_live_intent("operation-1", {"status": "failed"}, now=3)

    assert failed["state"] == "failed"
    assert store.get_policy("alice")["live_state"]["sweep_due"] == "100"
    replacement = _create_intent(store, "operation-2")
    assert replacement["state"] == "prepared"


def test_live_confirmation_updates_due_totals_and_daily_atomically(tmp_path: Path) -> None:
    """Confirmation applies accounting once and repeated reconciliation is idempotent."""

    store = _live_store(tmp_path)
    _create_intent(store, "operation-confirm", amount="25")
    store.transition_live_intent(
        "operation-confirm",
        "submitting",
        submission={"status": "submitted", "submitted_at_ms": 2_000},
        now=2,
    )
    confirmed = store.reconcile_live_intent(
        "operation-confirm", {"status": "confirmed"}, now=86_400
    )
    repeated = store.reconcile_live_intent(
        "operation-confirm", {"status": "confirmed"}, now=86_401
    )
    state = store.get_policy("alice")["live_state"]

    assert confirmed == repeated
    assert state["sweep_due"] == "75"
    assert state["confirmed_total"] == "25"
    assert state["daily_total"] == "25"


def test_unknown_retains_reservation_pauses_and_blocks_duplicates(tmp_path: Path) -> None:
    """An ambiguous result remains the sole reservation until reconciliation resolves it."""

    store = _live_store(tmp_path)
    _create_intent(store, "operation-unknown")
    store.transition_live_intent(
        "operation-unknown",
        "submitting",
        submission={"status": "unknown", "submitted_at_ms": 2_000},
        now=2,
    )
    unknown = store.reconcile_live_intent(
        "operation-unknown", {"status": "unknown", "reason": "ambiguous"}, now=3
    )

    assert unknown["state"] == "unknown"
    assert store.get_policy("alice")["policy"]["operating_mode"] == "paused_unknown"
    assert store.get_policy("alice")["live_state"]["sweep_due"] == "100"
    with pytest.raises((sqlite3.IntegrityError, ValueError)):
        _create_intent(store, "operation-duplicate")

    resolved = store.reconcile_live_intent(
        "operation-unknown", {"status": "confirmed"}, now=4
    )
    assert resolved["state"] == "confirmed"
    assert store.get_policy("alice")["policy"]["operating_mode"] == "live"


def test_submission_claims_are_atomic_for_live_and_test_operations(tmp_path: Path) -> None:
    """Only the first worker may claim a prepared operation for external submission."""

    store = _live_store(tmp_path)
    live = _create_intent(store, "claim-live")
    store.transition_live_intent(
        live["operation_id"],
        "submitting",
        submission={"status": "submitting"},
        claim=True,
        now=2,
    )
    with pytest.raises(ValueError, match="already been claimed"):
        store.transition_live_intent(
            live["operation_id"],
            "submitting",
            submission={"status": "submitting"},
            claim=True,
            now=3,
        )

    test = store.create_test_operation(
        "alice",
        operation_id="claim-test",
        parent_id=None,
        direction="forward",
        route="umfuture_to_funding",
        descriptor={
            "operation_id": "claim-test",
            "route": "umfuture_to_funding",
            "amount": "1",
        },
        requested_amount="1",
        now=2,
    )
    store.transition_test_operation(
        test["operation_id"],
        submission={"status": "submitting"},
        claim=True,
        now=2,
    )
    with pytest.raises(ValueError, match="already been claimed"):
        store.transition_test_operation(
            test["operation_id"],
            submission={"status": "submitting"},
            claim=True,
            now=3,
        )

    unresolved = store.list_unresolved_test_operations()
    assert [item["operation_id"] for item in unresolved] == ["claim-test"]


def test_stale_live_decision_cannot_reserve_after_accounting_changes(tmp_path: Path) -> None:
    """A second process cannot reuse a decision after another transfer changed Live totals."""

    store = ProfitSweepStore(tmp_path / "stale-reservation" / "state.sqlite3")
    store.create_policy(
        "alice",
        "binance",
        {
            "operating_mode": "dry",
            "baseline_mode": "lifetime",
            "trigger_percent": "0",
            "sweep_percent": "100",
            "live_minimum_transfer_amount": "1",
        },
    )
    store.activate_live("alice", "0", baseline_mode="include_dry_period")
    decision = store.evaluate_live(
        "alice", cumulative_net_pnl="100", max_transferable="100", now=1
    )
    descriptor = {
        "operation_id": "first-reservation",
        "route": "umfuture_to_funding",
        "amount": "25",
    }
    store.create_live_intent(
        "alice",
        operation_id="first-reservation",
        parent_id=None,
        leg=1,
        route="umfuture_to_funding",
        descriptor=descriptor,
        reserved_amount="25",
        reservation_guard=decision["reservation_guard"],
        now=2,
    )
    store.transition_live_intent(
        "first-reservation",
        "submitting",
        submission={"status": "submitted"},
        claim=True,
        now=2,
    )
    store.reconcile_live_intent("first-reservation", {"status": "confirmed"}, now=3)

    with pytest.raises(ValueError, match="decision is stale"):
        store.create_live_intent(
            "alice",
            operation_id="stale-reservation",
            parent_id=None,
            leg=1,
            route="umfuture_to_funding",
            descriptor={**descriptor, "operation_id": "stale-reservation"},
            reserved_amount="25",
            reservation_guard=decision["reservation_guard"],
            now=4,
        )


def test_policy_fingerprint_is_checked_inside_update_transaction(tmp_path: Path) -> None:
    """Two writers cannot both commit from the same reviewed policy revision."""

    store = _store(tmp_path)
    original = store.get_policy("alice")
    fingerprint = _policy_fingerprint(original["policy"])

    store.update_policy(
        "alice",
        {"quiet_period": 10},
        expected_policy_fingerprint=fingerprint,
    )

    with pytest.raises(ValueError, match="policy changed before update"):
        store.update_policy(
            "alice",
            {"quiet_period": 20},
            expected_policy_fingerprint=fingerprint,
        )
