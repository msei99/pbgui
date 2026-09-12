"""Offline overview cache, financial projection, and worker lifecycle regressions."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from profit_sweep import default_policy
from profit_sweep_overview import OverviewService, TargetReadCache, balance_projection, overview_row, snapshot_issues
from profit_sweep_overview import load_refresh_minutes, save_refresh_minutes, refresh_delay, needs_target_key_refresh


def record(name="alice", mode="live"):
    """Build isolated accounting state with deliberately different Dry and Live totals."""
    state = {"net_pnl": "4.67806097", "high_watermark": "6", "sweep_due": "0.339030485",
             "confirmed_total": "2", "last_decision": "below_high_watermark_recovery"}
    return {"user_name": name, "generation": 1, "exchange": "binance",
            "policy": {**default_policy(), "operating_mode": mode},
            "live_state": state, "simulation_state": {**state, "simulated_total": "99"}}


def snapshot(balance="1882.76509673"):
    """Return a synthetic exchange payload including fields that must never be persisted."""
    return {"account_balances": {"source": {"balance": balance},
            "destination": {"label": "Funding", "balance": "7", "secret": "DO-NOT-CACHE"}},
            "raw": "DO-NOT-CACHE", "collected_at_ms": 1000000}


def cache_identity():
    """Return the default synthetic account's balance-cache identity."""
    return {"generation": 1, "exchange": "binance", "asset": "USDT", "destination": "main_perps"}


def test_refresh_interval_defaults_persists_and_replaces_legacy_delay(tmp_path):
    """Default to 15 minutes and never reuse one-minute delays from old cache files."""
    path = tmp_path / "overview_settings.json"
    assert load_refresh_minutes(path) == 15
    assert refresh_delay("alice", {"refresh_delay": 60}, 900) >= 900
    save_refresh_minutes(path, 30)
    assert load_refresh_minutes(path) == 30
    assert path.stat().st_mode & 0o777 == 0o600
    assert refresh_delay("alice", {"refresh_delay": 60}, 1800) >= 1800


@pytest.mark.parametrize("minutes", [True, 1, 10, 0, -15, 1440, "15"])
def test_invalid_refresh_interval_does_not_overwrite_setting(tmp_path, minutes):
    """Reject unsupported intervals without damaging an existing saved preference."""
    path = tmp_path / "overview_settings.json"
    save_refresh_minutes(path, 15)
    with pytest.raises(ValueError):
        save_refresh_minutes(path, minutes)
    assert load_refresh_minutes(path) == 15


def test_stale_threshold_follows_refresh_interval(monkeypatch):
    """A ten-minute snapshot is fresh at 15 minutes but stale at five minutes."""
    import profit_sweep_overview as module
    monkeypatch.setattr(module.time, "time", lambda: 2000)
    cached = {**cache_identity(), "updated_at": 1400}
    assert not overview_row(record(), cached, refresh_seconds=900)["stale"]
    assert overview_row(record(), cached, refresh_seconds=300)["stale"]


@pytest.mark.parametrize("exchange,key,checked,expected", [
    ("hyperliquid", None, None, True),
    ("hyperliquid", None, True, False),
    ("hyperliquid", "identified", None, False),
    ("binance", None, None, False),
])
def test_target_identity_backfill_is_limited_to_legacy_hyperliquid(exchange, key, checked, expected):
    """Only an unattempted missing identity bypasses the configured refresh interval."""
    saved = record()
    saved["exchange"] = exchange
    assert needs_target_key_refresh(saved, {"target_key": key, "target_key_checked": checked}) is expected


@pytest.mark.parametrize("failed", [False, True])
def test_scheduler_backfills_fresh_legacy_cache_once_and_persists_attempt(tmp_path, monkeypatch, failed):
    """Backfill before 15 minutes, then honor normal scheduling even if the read failed."""
    async def exercise():
        """Run three scheduler ticks against fresh isolated cache data."""
        saved = record()
        saved["exchange"] = "hyperliquid"
        store = SimpleNamespace(db_path=tmp_path / "test.sqlite3", list_policies=lambda: [saved], get_policy=lambda _: saved)
        calls = []

        def collect(_record):
            """Return an identified destination or a transient read failure."""
            calls.append(1)
            if failed:
                raise TimeoutError("synthetic timeout")
            result = snapshot()
            result["account_balances"]["destination"]["overview_key"] = "a" * 64
            return result

        async def run_thread(function, *args):
            """Execute only isolated callbacks, without production worker threads."""
            return function(*args)

        service = OverviewService(store, collect, run_thread)
        import time
        service.cache["alice"] = {**cache_identity(), "exchange": "hyperliquid", "attempted_at": int(time.time()), "balance": "12", "updated_at": int(time.time())}
        original_sleep = asyncio.sleep
        ticks = []

        async def tick(_seconds):
            """Yield to refresh workers and terminate after three immediate scheduler ticks."""
            ticks.append(1)
            if len(ticks) == 3:
                raise asyncio.CancelledError
            await original_sleep(0)

        monkeypatch.setattr(asyncio, "sleep", tick)
        with pytest.raises(asyncio.CancelledError):
            await service._loop()
        assert calls == [1]
        persisted = OverviewService(store, collect, run_thread).cache["alice"]
        assert persisted["target_key_checked"] is True
        assert not needs_target_key_refresh(saved, persisted)
        assert persisted["error"] is failed
        await service.stop()

    asyncio.run(exercise())


@pytest.mark.parametrize("code,source,status", [
    ("unsupported_account_mode", "userAbstraction", "Account mode"),
    ("read_failed", "userFunding", "Read error"),
    ("invalid_response", "spotClearinghouseState", "Read error"),
])
def test_snapshot_failure_retains_cause_without_discarding_valid_balances(code, source, status):
    """Mode checks and failing history reads must not masquerade as missing balances."""
    issues = snapshot_issues({"errors": [{"code": code, "source": source, "message": "SECRET RAW RESPONSE"}]})
    row = overview_row(record(), {**cache_identity(), "balance": "108.55", "target_balance": "31.49", "error": True, "issues": issues})
    assert row["status"] == status
    assert row["balance"] == "108.55"
    assert row["target_balance"] == "31.49"
    assert row["issues"][0]["source"] == source
    assert row["issues"][0]["code"] == code
    assert "SECRET" not in json.dumps(row)


def test_unknown_error_payloads_are_not_exposed():
    """Diagnostic output uses only fixed allowlisted codes, sources, and descriptions."""
    issues = snapshot_issues({"errors": [{"code": "SECRET", "source": "SECRET", "message": "SECRET"}]})
    assert issues[0]["code"] == "snapshot_error"
    assert "SECRET" not in json.dumps(issues)


@pytest.mark.parametrize("failure", ["read_failed", "invalid_response"])
def test_failed_mode_read_is_not_reported_as_unsupported_mode(failure):
    """Transient mode-read failures do not prove that an account mode is incompatible."""
    issues = snapshot_issues({"errors": [
        {"code": failure, "source": "userAbstraction"},
        {"code": "unsupported_account_mode", "source": "userAbstraction"},
    ]})
    row = overview_row(record(), {**cache_identity(), "error": True, "issues": issues})
    assert row["status"] == "Read error"
    assert [issue["code"] for issue in row["issues"]] == [failure]


@pytest.mark.parametrize("mode,total", [("live", "2"), ("dry", "99"), ("paused_unknown", "2")])
def test_financial_projection_keeps_modes_recovery_and_precision(mode, total):
    """Use exact stored amounts and distinguish simulated from confirmed money."""
    row = overview_row(record(mode=mode), {**cache_identity(), **balance_projection(snapshot(), default_policy())})
    assert row["swept"] == total
    assert row["net_pnl"] == "4.67806097"
    assert row["recovery_needed"] == "1.32193903"
    assert row["status"] == ("Paused" if mode == "paused_unknown" else "Recovery")
    assert row["balance"] == "1882.76509673"
    assert row["stale"] is True
    assert "DO-NOT-CACHE" not in json.dumps(row)


def test_generation_change_drops_old_balances_and_missing_is_not_zero():
    """Never show previous policy-generation balances as current or replace unknown with zero."""
    row = overview_row(record(), {"generation": 0, "balance": "200", "target_balance": "9"})
    assert row["balance"] is None
    assert row["target_balance"] is None
    assert row["stale"] is True


def test_vault_target_uses_configured_destination_and_own_equity():
    """A vault row uses the leader's stake and the selected destination, not total vault TVL."""
    value = snapshot()
    value["account_kind"] = "vault"
    value["account_balances"]["destination"] = {
        "main_perps": {"label": "Main Perps", "balance": "7"},
        "main_spot": {"label": "Main Spot", "balance": "10"},
    }
    projected = balance_projection(value, {"vault_destination": "main_spot"})
    assert projected["target"] == "Main Spot"
    assert projected["target_balance"] == "10"
    assert projected["balance"] == "1882.76509673"


def test_independent_refresh_failure_persistence_and_shutdown(tmp_path):
    """A blocked account must not stop another; cache survives restart and workers are joined."""
    async def exercise():
        """Run deterministic workers against fake state, without network or production files."""
        records = [record("slow"), record("fast")]
        store = SimpleNamespace(db_path=tmp_path / "test.sqlite3", list_policies=lambda: records,
                                get_policy=lambda name: next(r for r in records if r["user_name"] == name))
        started = asyncio.Event()
        release = asyncio.Event()

        async def run_thread(function, *args):
            """Pause only the slow synthetic exchange read."""
            if function is collect and args[0]["user_name"] == "slow":
                started.set()
                await release.wait()
            return function(*args)

        def collect(_record):
            """Supply display values only."""
            return snapshot()

        service = OverviewService(store, collect, run_thread)
        slow = asyncio.create_task(service._refresh(records[0]))
        service.workers["slow"] = slow
        await started.wait()
        await service._refresh(records[1])
        assert service.cache["fast"]["balance"] == "1882.76509673"
        assert not slow.done()
        persisted = Path(service.path).read_text()
        assert "DO-NOT-CACHE" not in persisted
        assert Path(service.path).stat().st_mode & 0o777 == 0o600
        assert OverviewService(store, collect, run_thread).cache["fast"]["target_balance"] == "7"
        await service.stop()
        await service.stop()
        assert slow.done()
        assert not service.workers

    asyncio.run(exercise())


def test_failed_refresh_preserves_last_successful_balance_and_date(tmp_path):
    """Read failures expose an error while preserving the last known balance timestamp."""
    async def exercise():
        """Simulate an offline exchange failure with an existing cache."""
        saved = record()
        store = SimpleNamespace(db_path=tmp_path / "test.sqlite3", list_policies=lambda: [saved], get_policy=lambda _: saved)

        def fail(_record):
            """Fail without exposing diagnostic secrets."""
            raise RuntimeError("DO-NOT-CACHE")

        async def run_thread(function, *args):
            """Execute isolated callbacks deterministically."""
            return function(*args)

        service = OverviewService(store, fail, run_thread)
        service.cache["alice"] = {**cache_identity(), "balance": "12", "updated_at": 42}
        await service._refresh(saved)
        row = overview_row(saved, service.cache["alice"])
        assert row["balance"] == "12"
        assert row["updated_at"] == 42
        assert row["status"] == "Read error"
        assert "DO-NOT-CACHE" not in Path(service.path).read_text()

    asyncio.run(exercise())


def test_partial_refresh_keeps_each_balance_date_and_backs_off(tmp_path):
    """Fresh target data must survive a failed source read, while failures slow subsequent refreshes."""
    async def exercise():
        """Run partial and recovered snapshots against isolated state."""
        saved = record()
        store = SimpleNamespace(db_path=tmp_path / "test.sqlite3", list_policies=lambda: [saved], get_policy=lambda _: saved)
        result = snapshot(None)
        result["errors"] = [{"code": "read_timeout", "source": "clearinghouseState"}]

        async def run_thread(function, *args):
            """Run only synthetic local callbacks."""
            return function(*args)

        service = OverviewService(store, lambda _: result, run_thread)
        service.cache["alice"] = {**cache_identity(), "balance": "12", "target_balance": "3", "updated_at": 42}
        await service._refresh(saved)
        first = service.cache["alice"]
        assert first["balance"] == "12"
        assert first["source_updated_at"] == 42
        assert first["target_balance"] == "7"
        assert first["target_updated_at"] == 1000
        await service._refresh(saved)
        assert service.cache["alice"]["refresh_delay"] > first["refresh_delay"]
        result = snapshot("15")
        await service._refresh(saved)
        assert service.cache["alice"]["failure_count"] == 0
        assert service.cache["alice"]["error"] is False
        assert service.cache["alice"]["balance"] == "15"

    asyncio.run(exercise())


@pytest.mark.parametrize("field,value", [("asset", "USDC"), ("vault_destination", "main_spot")])
def test_same_generation_asset_or_destination_change_invalidates_balances(field, value):
    """Do not display money from another asset or wallet after editing the policy."""
    saved = record()
    saved["policy"][field] = value
    row = overview_row(saved, {**cache_identity(), "balance": "12", "target_balance": "9"})
    assert row["balance"] is None
    assert row["target_balance"] is None


def test_shared_target_cache_is_scoped_to_overview_and_expires(monkeypatch):
    """Shared display destinations are cached; actual transfer checks always read afresh."""
    import profit_sweep_exchanges as exchanges
    import profit_sweep_overview as overview

    calls = []
    clock = [100.0]

    def fetch(request_type, **kwargs):
        """Count synthetic reads of a fixed destination."""
        calls.append(request_type)
        return {"marginSummary": {"accountValue": "12"}}

    monkeypatch.setattr(exchanges, "hyperliquid_readonly_info", fetch)
    monkeypatch.setattr(overview.time, "monotonic", lambda: clock[0])
    cache = TargetReadCache()
    token = exchanges.OVERVIEW_TARGET_CACHE.set(cache)
    try:
        for _ in range(2):
            result = exchanges._optional_hyperliquid_read("clearinghouseState", expected_type=dict, timeout_s=1, user="leader")
            assert result["marginSummary"]["accountValue"] == "12"
            result["marginSummary"]["accountValue"] = "changed"
        assert len(calls) == 1
        clock[0] += 61
        exchanges._optional_hyperliquid_read("clearinghouseState", expected_type=dict, timeout_s=1, user="leader")
        assert len(calls) == 2
    finally:
        exchanges.OVERVIEW_TARGET_CACHE.reset(token)
    exchanges._optional_hyperliquid_read("clearinghouseState", expected_type=dict, timeout_s=1, user="leader")
    assert len(calls) == 3
    cache.clear()
    assert not cache.entries


def test_manual_refresh_queues_fresh_accounts_once_and_coalesces_clicks(tmp_path, monkeypatch):
    """A manual refresh bypasses the interval, but ticks and repeated clicks do not duplicate reads."""
    async def exercise():
        """Run only synthetic account reads through the real scheduler."""
        saved = record()
        calls = []
        store = SimpleNamespace(db_path=tmp_path / 'test.sqlite3', list_policies=lambda: [saved], get_policy=lambda _: saved)

        async def run_thread(function, *args):
            """Execute isolated callbacks directly."""
            return function(*args)

        def collect(_record):
            """Record one synthetic balance read."""
            calls.append(1)
            return snapshot()

        service = OverviewService(store, collect, run_thread)
        monkeypatch.setattr('profit_sweep_overview.time.time', lambda: 10000)
        service.cache['alice'] = {**cache_identity(), 'attempted_at': 9990}
        service.request_refresh()
        assert service.refresh_requested_at == 10000
        monkeypatch.setattr('profit_sweep_overview.time.time', lambda: 10005)
        service.request_refresh()
        assert service.refresh_requested_at == 10000
        original_sleep = asyncio.sleep
        ticks = []

        async def tick(_seconds):
            """Allow workers to run and stop after three ticks."""
            ticks.append(1)
            if len(ticks) == 3:
                raise asyncio.CancelledError
            await original_sleep(0)

        monkeypatch.setattr(asyncio, 'sleep', tick)
        with pytest.raises(asyncio.CancelledError):
            await service._loop()
        assert calls == [1]
        assert service.cache['alice']['attempted_at'] == 10005
        await service.stop()

    asyncio.run(exercise())
