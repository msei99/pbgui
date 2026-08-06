"""Tests for OHLCV integrity task-worker dispatch and repair."""

from __future__ import annotations

from contextlib import nullcontext
import json
from pathlib import Path
from types import SimpleNamespace

import market_data_integrity
import pytest
import task_worker


def _write_job(path: Path, job_type: str, payload: dict) -> None:
    """Write a minimal persisted worker job."""
    path.write_text(
        json.dumps({"id": path.stem, "type": job_type, "payload": payload, "status": "pending"}),
        encoding="utf-8",
    )


def _install_direct_job_updates(monkeypatch) -> list[str]:
    """Keep test job transitions local to the temporary job file."""
    moved = []

    def update(path, mutate):
        obj = json.loads(path.read_text(encoding="utf-8"))
        mutate(obj)
        path.write_text(json.dumps(obj), encoding="utf-8")
        return obj

    monkeypatch.setattr(task_worker, "update_job_file", update)
    monkeypatch.setattr(task_worker, "move_job_file", lambda _path, state: moved.append(state))
    monkeypatch.setattr(task_worker, "integrity_job_lock", lambda: nullcontext())
    return moved


def test_run_job_dispatches_integrity_scan(monkeypatch, tmp_path: Path) -> None:
    """The generic worker recognizes the durable integrity scan job type."""
    job_path = tmp_path / "scan.json"
    _write_job(job_path, "ohlcv_integrity_scan", {"exchange": "bybit"})
    moved = _install_direct_job_updates(monkeypatch)
    calls = []
    monkeypatch.setattr(task_worker, "_run_ohlcv_integrity_scan", lambda path, payload: calls.append((path, payload)))

    task_worker._run_job(job_path)

    assert calls == [(job_path, {"exchange": "bybit"})]
    assert moved == ["done"]


def test_run_job_dispatches_hyperliquid_fallback_normalization(monkeypatch, tmp_path: Path) -> None:
    """The worker dispatches normalization under the shared integrity job lock."""
    job_path = tmp_path / "normalize.json"
    payload = {"exchange": "hyperliquid", "dry_run": False}
    _write_job(job_path, "ohlcv_hyperliquid_normalize_fallback", payload)
    moved = _install_direct_job_updates(monkeypatch)
    calls = []
    monkeypatch.setattr(
        task_worker,
        "_run_ohlcv_hyperliquid_normalize_fallback",
        lambda path, value: calls.append((path, value)),
    )

    task_worker._run_job(job_path)

    assert calls == [(job_path, payload)]
    assert moved == ["done"]


@pytest.mark.parametrize("exchange", task_worker.SUPPORTED_EXCHANGES)
def test_integrity_scan_runner_uses_exact_storage_exchange(monkeypatch, tmp_path: Path, exchange: str) -> None:
    """The worker forwards every supported storage exchange without alias drift."""
    job_path = tmp_path / f"scan-{exchange}.json"
    _write_job(job_path, "ohlcv_integrity_scan", {"exchange": exchange})
    _install_direct_job_updates(monkeypatch)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda *_args: None)
    calls = []
    monkeypatch.setattr(
        task_worker,
        "scan_exchange",
        lambda value, **_kwargs: calls.append(value) or {
            "files_scanned": 1,
            "invalid_days": 0,
        },
    )

    task_worker._run_ohlcv_integrity_scan(job_path, {"exchange": exchange})

    assert calls == [exchange]


def test_integrity_scan_runner_rejects_missing_exchange(tmp_path: Path) -> None:
    """Persisted scan jobs cannot silently fall back to Bybit."""
    job_path = tmp_path / "scan.json"
    _write_job(job_path, "ohlcv_integrity_scan", {})

    with pytest.raises(ValueError, match="Unsupported integrity scan exchange"):
        task_worker._run_ohlcv_integrity_scan(job_path, {})


def test_run_job_dispatches_integrity_repair_all(monkeypatch, tmp_path: Path) -> None:
    """The generic worker recognizes the sequential repair-all job type."""
    job_path = tmp_path / "repair-all.json"
    _write_job(job_path, "ohlcv_integrity_repair_all", {"exchange": "bybit"})
    moved = _install_direct_job_updates(monkeypatch)
    calls = []
    monkeypatch.setattr(task_worker, "_run_ohlcv_integrity_repair_all", lambda path, payload: calls.append((path, payload)))

    task_worker._run_job(job_path)

    assert calls == [(job_path, {"exchange": "bybit"})]
    assert moved == ["done"]


def test_run_job_dispatches_removed_coin_delete(monkeypatch, tmp_path: Path) -> None:
    """The generic worker recognizes removed-coin deletion jobs."""
    job_path = tmp_path / "remove.json"
    payload = {"exchange": "bybit", "coin": "OLD_USDT:USDT"}
    _write_job(job_path, "ohlcv_removed_coin_delete", payload)
    moved = _install_direct_job_updates(monkeypatch)
    calls = []
    monkeypatch.setattr(task_worker, "_run_ohlcv_removed_coin_delete", lambda path, value: calls.append((path, value)))

    task_worker._run_job(job_path)

    assert calls == [(job_path, payload)]
    assert moved == ["done"]


def test_run_job_dispatches_removed_coins_batch_delete(monkeypatch, tmp_path: Path) -> None:
    """The generic worker recognizes unavailable-market batch deletion jobs."""
    job_path = tmp_path / "remove-batch.json"
    payload = {"exchange": "bybit", "coins": ["OLD_A_USDT:USDT", "OLD_B_USDT:USDT"]}
    _write_job(job_path, "ohlcv_removed_coins_delete", payload)
    moved = _install_direct_job_updates(monkeypatch)
    calls = []
    monkeypatch.setattr(task_worker, "_run_ohlcv_removed_coins_delete", lambda path, value: calls.append((path, value)))

    task_worker._run_job(job_path)

    assert calls == [(job_path, payload)]
    assert moved == ["done"]


def test_repair_all_partial_result_is_preserved_as_failed(monkeypatch, tmp_path: Path) -> None:
    """Remaining day failures put the batch in Failed without losing successes."""
    job_path = tmp_path / "repair-all.json"
    _write_job(job_path, "ohlcv_integrity_repair_all", {"exchange": "bybit"})
    moved = _install_direct_job_updates(monkeypatch)

    def partial(path, _payload):
        task_worker.update_job_file(path, mutate=lambda obj: obj.update({"result": {"repaired": 10, "failed": 2}}))
        raise RuntimeError("Repair all completed partially with 2 failed day(s)")

    monkeypatch.setattr(task_worker, "_run_ohlcv_integrity_repair_all", partial)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda *_args: None)

    task_worker._run_job(job_path)

    saved = json.loads(job_path.read_text())
    assert moved == ["failed"]
    assert saved["status"] == "failed"
    assert saved["result"] == {"repaired": 10, "failed": 2}


def test_integrity_repair_finalizes_and_records_day(monkeypatch, tmp_path: Path) -> None:
    """Repair preserves base names ending in quote-like currency text."""
    job_path = tmp_path / "repair.json"
    _write_job(
        job_path,
        "ohlcv_integrity_repair",
        {"exchange": "bybit", "coin": "RLUSD_USDT:USDT", "day": "2026-01-02"},
    )
    _install_direct_job_updates(monkeypatch)
    day_path = tmp_path / "2026-01-02.npz"
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda *_args: None)
    monkeypatch.setattr(task_worker, "bybit_storage_market_status", lambda _coin: {"status": "available"})
    monkeypatch.setattr(task_worker, "catalog_operation_lock", lambda: nullcontext())
    finalized = []
    monkeypatch.setattr(
        task_worker,
        "finalize_bybit_1m_day_for_coin",
        lambda **kwargs: finalized.append(kwargs) or {"result": "ok", "minutes_written": 1440, "path": str(day_path)},
    )
    validation = market_data_integrity.DayValidation(
        status="valid",
        candles=1440,
        missing_minutes=0,
        sha256="a" * 64,
        first_ts=1,
        last_ts=2,
    )
    recorded = []
    monkeypatch.setattr(
        task_worker,
        "record_daily_file",
        lambda **kwargs: recorded.append(kwargs) or validation,
    )

    task_worker._run_ohlcv_integrity_repair(job_path, json.loads(job_path.read_text())["payload"])

    saved = json.loads(job_path.read_text())
    assert finalized[0]["coin"] == "RLUSD"
    assert finalized[0]["verify_inception_independently"] is True
    assert recorded[0]["coin"] == "RLUSD_USDT:USDT"
    assert saved["progress"]["stage"] == "done"
    assert saved["result"]["validation"]["status"] == "valid"


def test_bybit_known_source_gap_is_recorded_without_refetch(monkeypatch, tmp_path: Path) -> None:
    """Repair accepts only the exact registered Bybit source gap without a futile API rewrite."""
    day_path = tmp_path / "2021-01-11.npz"
    monkeypatch.setattr(task_worker, "bybit_storage_market_status", lambda _coin: {"status": "available"})
    monkeypatch.setattr(task_worker, "get_bybit_day_path", lambda _coin, _day: day_path)
    monkeypatch.setattr(task_worker, "known_source_gap_minutes", lambda *_args: frozenset({350}))
    monkeypatch.setattr(
        task_worker,
        "finalize_bybit_1m_day_for_coin",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("known source gap was refetched")),
    )
    validation = market_data_integrity.DayValidation(
        status="source_gap",
        candles=1439,
        missing_minutes=1,
        sha256="a" * 64,
        first_ts=1,
        last_ts=2,
    )
    recorded = []
    monkeypatch.setattr(
        task_worker,
        "record_daily_file",
        lambda **kwargs: recorded.append(kwargs) or validation,
    )

    result = task_worker._repair_ohlcv_integrity_day(
        exchange="bybit",
        storage_coin="XTZ_USDT:USDT",
        day="2021-01-11",
    )

    assert recorded[0]["allowed_source_gap_minutes"] == frozenset({350})
    assert result["repair"]["result"] == "known_source_gap"
    assert result["validation"]["status"] == "source_gap"


def test_hyperliquid_integrity_repair_improves_exact_day(monkeypatch, tmp_path: Path) -> None:
    """Hyperliquid repair runs the existing L2/API/Binance/Bybit improve path for one day."""
    monkeypatch.setattr(
        task_worker,
        "storage_market_status",
        lambda _exchange, _coin: {"status": "available", "reason": "active"},
    )
    monkeypatch.setattr(task_worker, "get_market_data_root_dir", lambda: tmp_path)
    monkeypatch.setattr(task_worker, "repair_coin_from_storage", lambda _exchange, _coin: "BLAST")
    improved = []
    repair_result = SimpleNamespace(to_dict=lambda: {"days_checked": 1, "bybit_minutes_filled": 20})
    monkeypatch.setattr(
        task_worker,
        "improve_best_hyperliquid_1m_archive_for_coin",
        lambda **kwargs: improved.append(kwargs) or repair_result,
    )
    validation = market_data_integrity.DayValidation(
        status="valid",
        candles=1440,
        missing_minutes=0,
        sha256="a" * 64,
        first_ts=1,
        last_ts=2,
    )
    recorded = []
    monkeypatch.setattr(
        task_worker,
        "record_daily_file",
        lambda **kwargs: recorded.append(kwargs) or validation,
    )

    result = task_worker._repair_ohlcv_integrity_day(
        exchange="hyperliquid",
        storage_coin="BLAST_USDC:USDC",
        day="2025-12-26",
    )

    assert improved == [{
        "coin": "BLAST",
        "start_date_override": "2025-12-26",
        "end_date": "2025-12-26",
        "dry_run": False,
        "refetch": False,
        "archive_l2book": False,
    }]
    assert recorded[0]["path"] == tmp_path / "hyperliquid" / "1m" / "BLAST_USDC:USDC" / "2025-12-26.npz"
    assert result["repair"]["bybit_minutes_filled"] == 20


def test_hyperliquid_integrity_repair_catalogs_proven_source_gap(monkeypatch, tmp_path: Path) -> None:
    """An exact historical gap leaves the Repair Queue only after archive and donor repair attempts."""
    monkeypatch.setattr(
        task_worker,
        "storage_market_status",
        lambda _exchange, _coin: {"status": "available", "reason": "active"},
    )
    monkeypatch.setattr(task_worker, "get_market_data_root_dir", lambda: tmp_path)
    monkeypatch.setattr(task_worker, "repair_coin_from_storage", lambda _exchange, _coin: "PURR")
    repair_result = SimpleNamespace(to_dict=lambda: {"days_checked": 1, "binance_minutes_filled": 0})
    monkeypatch.setattr(task_worker, "improve_best_hyperliquid_1m_archive_for_coin", lambda **_kwargs: repair_result)
    invalid = market_data_integrity.DayValidation(
        status="invalid", candles=1200, missing_minutes=240, sha256="", first_ts=None, last_ts=None,
        error="internal minute gap",
    )
    source_gap = market_data_integrity.DayValidation(
        status="source_gap", candles=1200, missing_minutes=240, sha256="a" * 64, first_ts=1, last_ts=2,
    )
    monkeypatch.setattr(task_worker, "record_daily_file", lambda **_kwargs: invalid)
    monkeypatch.setattr(task_worker, "daily_missing_minutes", lambda *_args: set(range(240)))
    monkeypatch.setattr(
        task_worker,
        "classify_hyperliquid_pre_donor_gap",
        lambda **_kwargs: {"eligible": True, "reason": "pre-donor", "sources": {}},
    )
    recorded = []
    monkeypatch.setattr(
        task_worker,
        "record_proven_source_gap",
        lambda **kwargs: recorded.append(kwargs) or source_gap,
    )

    result = task_worker._repair_ohlcv_integrity_day(
        exchange="hyperliquid",
        storage_coin="PURR_USDC:USDC",
        day="2024-11-13",
    )

    assert recorded[0]["coin"] == "PURR_USDC:USDC"
    assert result["validation"]["status"] == "source_gap"
    assert result["source_gap"]["eligible"] is True


def test_hyperliquid_repair_accepts_gap_after_complete_empty_donor_queries(monkeypatch, tmp_path: Path) -> None:
    """A theoretical launch boundary yields to successful donor queries returning no missing candles."""
    monkeypatch.setattr(
        task_worker,
        "storage_market_status",
        lambda _exchange, _coin: {"status": "available", "reason": "active"},
    )
    monkeypatch.setattr(task_worker, "get_market_data_root_dir", lambda: tmp_path)
    monkeypatch.setattr(task_worker, "repair_coin_from_storage", lambda _exchange, _coin: "JTO")
    repair_payload = {
        "days_checked": 1,
        "binance_days_requested": 1,
        "binance_days_completed": 1,
        "binance_fetch_errors": 0,
        "bybit_days_requested": 1,
        "bybit_days_completed": 1,
        "bybit_fetch_errors": 0,
    }
    monkeypatch.setattr(
        task_worker,
        "improve_best_hyperliquid_1m_archive_for_coin",
        lambda **_kwargs: SimpleNamespace(to_dict=lambda: repair_payload),
    )
    invalid = market_data_integrity.DayValidation(
        status="invalid", candles=454, missing_minutes=986, sha256="", first_ts=None, last_ts=None,
        error="internal minute gap",
    )
    source_gap = market_data_integrity.DayValidation(
        status="source_gap", candles=454, missing_minutes=986, sha256="a" * 64, first_ts=1, last_ts=2,
    )
    monkeypatch.setattr(task_worker, "record_daily_file", lambda **_kwargs: invalid)
    monkeypatch.setattr(task_worker, "daily_missing_minutes", lambda *_args: set(range(986)))
    monkeypatch.setattr(
        task_worker,
        "classify_hyperliquid_pre_donor_gap",
        lambda **_kwargs: {
            "eligible": False,
            "reason": "bybit may cover",
            "sources": {
                "binanceusdm": {"status": "available"},
                "bybit": {"status": "available"},
            },
        },
    )
    monkeypatch.setattr(task_worker, "record_proven_source_gap", lambda **_kwargs: source_gap)

    result = task_worker._repair_ohlcv_integrity_day(
        exchange="hyperliquid",
        storage_coin="JTO_USDC:USDC",
        day="2023-12-07",
    )

    assert result["validation"]["status"] == "source_gap"
    assert result["source_gap"]["reason"] == "external donor queries completed without the remaining minutes"


def test_hyperliquid_repair_normalizes_historical_fallback_before_source_gap(monkeypatch, tmp_path: Path) -> None:
    """Existing other-exchange envelopes are repaired before remaining timestamps are classified."""
    monkeypatch.setattr(
        task_worker,
        "storage_market_status",
        lambda _exchange, _coin: {"status": "available", "reason": "active"},
    )
    monkeypatch.setattr(task_worker, "get_market_data_root_dir", lambda: tmp_path)
    monkeypatch.setattr(task_worker, "repair_coin_from_storage", lambda _exchange, _coin: "LIT")
    monkeypatch.setattr(
        task_worker,
        "improve_best_hyperliquid_1m_archive_for_coin",
        lambda **_kwargs: SimpleNamespace(to_dict=lambda: {"days_checked": 1}),
    )
    gap_invalid = market_data_integrity.DayValidation(
        status="invalid", candles=1431, missing_minutes=9, sha256="", first_ts=None, last_ts=None,
        error="internal minute gap",
    )
    validations = iter((gap_invalid, gap_invalid))
    monkeypatch.setattr(task_worker, "record_daily_file", lambda **_kwargs: next(validations))
    normalized = []
    monkeypatch.setattr(
        task_worker,
        "normalize_hyperliquid_fallback_envelopes",
        lambda **kwargs: normalized.append(kwargs) or {"files_changed": 1, "candles_changed": 1},
    )
    monkeypatch.setattr(task_worker, "daily_missing_minutes", lambda *_args: set(range(9)))
    monkeypatch.setattr(
        task_worker,
        "classify_hyperliquid_pre_donor_gap",
        lambda **_kwargs: {"eligible": True, "reason": "pre-donor", "sources": {}},
    )
    source_gap = market_data_integrity.DayValidation(
        status="source_gap", candles=1431, missing_minutes=9, sha256="a" * 64, first_ts=1, last_ts=2,
    )
    monkeypatch.setattr(task_worker, "record_proven_source_gap", lambda **_kwargs: source_gap)

    result = task_worker._repair_ohlcv_integrity_day(
        exchange="hyperliquid",
        storage_coin="LIT_USDC:USDC",
        day="2025-12-23",
    )

    assert normalized == [{"coin": "LIT_USDC:USDC", "day": "2025-12-23"}]
    assert result["normalization"]["candles_changed"] == 1
    assert result["validation"]["status"] == "source_gap"


@pytest.mark.parametrize(
    ("exchange", "storage_coin", "builder_name", "builder_coin"),
    [
        ("binanceusdm", "1000SHIB_USDT:USDT", "improve_best_binance_1m_for_coin", "SHIB"),
        ("okx", "BTC_USDT:USDT", "improve_best_okx_1m_for_coin", "BTC"),
        ("bitget", "BTC_USDT:USDT", "improve_best_bitget_1m_for_coin", "BTC"),
    ],
)
def test_other_exchange_integrity_repair_refetches_exact_day(
    monkeypatch,
    tmp_path: Path,
    exchange: str,
    storage_coin: str,
    builder_name: str,
    builder_coin: str,
) -> None:
    """Binance, OKX, and Bitget repairs refetch only the requested damaged day."""
    monkeypatch.setattr(
        task_worker,
        "storage_market_status",
        lambda _exchange, _coin: {"status": "available", "reason": "active"},
    )
    monkeypatch.setattr(task_worker, "repair_coin_from_storage", lambda _exchange, _coin: builder_coin)
    monkeypatch.setattr(task_worker, "get_current_market_inception_ms", lambda _coin: None)
    monkeypatch.setattr(task_worker, "get_market_data_root_dir", lambda: tmp_path)
    calls = []
    result_obj = SimpleNamespace(to_dict=lambda: {"days_checked": 1, "minutes_written": 1440})
    monkeypatch.setattr(task_worker, builder_name, lambda **kwargs: calls.append(kwargs) or result_obj)
    validation = market_data_integrity.DayValidation(
        status="valid", candles=1440, missing_minutes=0, sha256="a" * 64, first_ts=1, last_ts=2
    )
    monkeypatch.setattr(task_worker, "record_daily_file", lambda **_kwargs: validation)

    result = task_worker._repair_ohlcv_integrity_day(
        exchange=exchange,
        storage_coin=storage_coin,
        day="2026-01-02",
    )

    assert calls == [{
        "coin": builder_coin,
        "start_date_override": "2026-01-02",
        "end_date": "2026-01-02",
        "refetch": True,
    }]
    assert result["validation"]["status"] == "valid"


def test_binance_relaunch_repair_removes_obsolete_generation(monkeypatch, tmp_path: Path) -> None:
    """Repair retains the current Binance launch day and removes reused-symbol history before it."""
    coin_dir = tmp_path / "binanceusdm" / "1m" / "AIA_USDT:USDT"
    coin_dir.mkdir(parents=True)
    old_day = coin_dir / "2026-01-19.npz"
    launch_day = coin_dir / "2026-01-20.npz"
    old_day.write_bytes(b"old generation")
    launch_day.write_bytes(b"current generation")
    monkeypatch.setattr(
        task_worker,
        "storage_market_status",
        lambda _exchange, _coin: {"status": "available", "reason": "active"},
    )
    monkeypatch.setattr(task_worker, "repair_coin_from_storage", lambda _exchange, _coin: "AIA")
    monkeypatch.setattr(task_worker, "get_market_data_root_dir", lambda: tmp_path)
    monkeypatch.setattr(task_worker, "get_current_market_inception_ms", lambda _coin: 1768907700000)
    monkeypatch.setattr(
        task_worker,
        "improve_best_binance_1m_for_coin",
        lambda **_kwargs: SimpleNamespace(to_dict=lambda: {"days_checked": 1, "minutes_written": 765}),
    )
    monkeypatch.setattr(task_worker, "catalog_operation_lock", lambda: nullcontext())
    removed_sources = []
    monkeypatch.setattr(task_worker, "remove_days_from_index", lambda **kwargs: removed_sources.append(kwargs) or 1)
    monkeypatch.setattr(task_worker, "remove_catalog_before_day", lambda **_kwargs: 1)
    recorded = []
    validation = market_data_integrity.DayValidation(
        status="inception_partial",
        candles=765,
        missing_minutes=0,
        sha256="a" * 64,
        first_ts=1,
        last_ts=2,
    )
    monkeypatch.setattr(
        task_worker,
        "record_daily_file",
        lambda **kwargs: recorded.append(kwargs) or validation,
    )

    result = task_worker._repair_ohlcv_integrity_day(
        exchange="binanceusdm",
        storage_coin="AIA_USDT:USDT",
        day="2026-01-20",
    )

    assert not old_day.exists()
    assert launch_day.exists()
    assert recorded[0]["allow_inception_prefix"] is True
    assert result["inception_day"] == "2026-01-20"
    assert result["removed_pre_inception"] == 1
    assert removed_sources[0]["days_to_remove"] == {"2026-01-19"}


def test_integrity_repair_all_continues_after_individual_failure(monkeypatch, tmp_path: Path) -> None:
    """Batch repair records failures without abandoning later damaged days."""
    job_path = tmp_path / "repair-all.json"
    _write_job(job_path, "ohlcv_integrity_repair_all", {"exchange": "bybit"})
    _install_direct_job_updates(monkeypatch)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda *_args: None)
    monkeypatch.setattr(
        task_worker,
        "list_integrity_issues",
        lambda **_kwargs: {
            "rows": [
                {"coin": "BAD_USDT:USDT", "day": "2026-01-01"},
                {"coin": "GOOD_USDT:USDT", "day": "2026-01-02"},
            ]
        },
    )

    def repair(*, exchange, storage_coin, day):
        if storage_coin.startswith("BAD"):
            raise RuntimeError("not available")
        return {"validation": {"status": "valid", "candles": 1440}}

    monkeypatch.setattr(task_worker, "_repair_ohlcv_integrity_day", repair)

    with pytest.raises(RuntimeError, match="partially with 1 failed"):
        task_worker._run_ohlcv_integrity_repair_all(job_path, {"exchange": "bybit"})

    saved = json.loads(job_path.read_text())
    assert saved["result"]["total"] == 2
    assert saved["result"]["repaired"] == 1
    assert saved["result"]["failed"] == 1
    assert saved["progress"]["stage"] == "done"


def test_integrity_repair_removes_verified_pre_inception_generation(monkeypatch, tmp_path: Path) -> None:
    """A proven newer inception removes the complete obsolete local generation."""
    old_day = tmp_path / "2026-01-01.npz"
    repair_day = tmp_path / "2026-01-02.npz"
    inception_day = tmp_path / "2026-01-03.npz"
    old_day.write_bytes(b"old")
    repair_day.write_bytes(b"damaged")
    inception_day.write_bytes(b"current")
    monkeypatch.setattr(task_worker, "bybit_storage_market_status", lambda _coin: {"status": "available"})
    monkeypatch.setattr(task_worker, "catalog_operation_lock", lambda: nullcontext())
    monkeypatch.setattr(
        task_worker,
        "finalize_bybit_1m_day_for_coin",
        lambda **_kwargs: {"result": "not_applicable", "inception_day": "2026-01-03"},
    )
    monkeypatch.setattr(task_worker, "get_bybit_day_path", lambda _coin, day: tmp_path / f"{day}.npz")
    removed_sources = []
    monkeypatch.setattr(task_worker, "remove_days_from_index", lambda **kwargs: removed_sources.append(kwargs) or 1)
    removed_catalog = []
    monkeypatch.setattr(
        task_worker,
        "remove_catalog_before_day",
        lambda **kwargs: removed_catalog.append(kwargs) or 2,
    )

    result = task_worker._repair_ohlcv_integrity_day(
        exchange="bybit",
        storage_coin="NEW_USDT:USDT",
        day="2026-01-02",
    )

    assert result["validation"]["status"] == "not_applicable"
    assert result["inception_day"] == "2026-01-03"
    assert result["removed_pre_inception"] == 2
    assert result["removed_catalog_rows"] == 2
    assert not old_day.exists()
    assert not repair_day.exists()
    assert inception_day.exists()
    assert removed_sources[0]["days_to_remove"] == {"2026-01-01", "2026-01-02"}
    assert removed_catalog[0]["before_day"].isoformat() == "2026-01-03"


def test_integrity_repair_all_skips_removed_markets(monkeypatch, tmp_path: Path) -> None:
    """Repair All reports unavailable markets without attempting a download."""
    job_path = tmp_path / "repair-all.json"
    _write_job(job_path, "ohlcv_integrity_repair_all", {"exchange": "bybit"})
    _install_direct_job_updates(monkeypatch)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda *_args: None)
    monkeypatch.setattr(
        task_worker,
        "list_integrity_issues",
        lambda **_kwargs: {
            "rows": [{"coin": "OLD_USDT:USDT", "day": "2026-01-01", "market_status": "removed"}]
        },
    )
    monkeypatch.setattr(
        task_worker,
        "_repair_ohlcv_integrity_day",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("removed market was repaired")),
    )

    task_worker._run_ohlcv_integrity_repair_all(job_path, {"exchange": "bybit"})

    saved = json.loads(job_path.read_text())
    assert saved["result"]["repaired"] == 0
    assert saved["result"]["failed"] == 0
    assert saved["result"]["skipped_unavailable"] == 1


def test_integrity_repair_all_reports_complete_removed_generation(monkeypatch, tmp_path: Path) -> None:
    """Batch results count every obsolete file removed after inception verification."""
    job_path = tmp_path / "repair-generation.json"
    payload = {"exchange": "bybit", "coin": "KORU_USDT:USDT"}
    _write_job(job_path, "ohlcv_integrity_repair_all", payload)
    _install_direct_job_updates(monkeypatch)
    log_lines = []
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda _job_id, message: log_lines.append(message))
    monkeypatch.setattr(
        task_worker,
        "list_integrity_issues",
        lambda **_kwargs: {
            "rows": [{"coin": "KORU_USDT:USDT", "day": "2026-07-13", "market_status": "available"}]
        },
    )
    monkeypatch.setattr(
        task_worker,
        "_repair_ohlcv_integrity_day",
        lambda **_kwargs: {
            "validation": {"status": "not_applicable"},
            "inception_day": "2026-07-15",
            "removed_pre_inception": 20,
        },
    )

    task_worker._run_ohlcv_integrity_repair_all(job_path, payload)

    saved = json.loads(job_path.read_text())
    assert saved["result"]["repaired"] == 0
    assert saved["result"]["removed_pre_inception"] == 20
    assert any("Removed 20 obsolete local day(s)" in line for line in log_lines)


def test_integrity_repair_all_counts_source_gaps_separately(monkeypatch, tmp_path: Path) -> None:
    """Verified unavailable history is not reported as a repaired candle day."""
    job_path = tmp_path / "repair-source-gap.json"
    payload = {"exchange": "hyperliquid", "coin": "PURR_USDC:USDC"}
    _write_job(job_path, "ohlcv_integrity_repair_all", payload)
    _install_direct_job_updates(monkeypatch)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda *_args: None)
    monkeypatch.setattr(
        task_worker,
        "list_integrity_issues",
        lambda **_kwargs: {
            "rows": [{"coin": "PURR_USDC:USDC", "day": "2024-11-13", "market_status": "available"}]
        },
    )
    monkeypatch.setattr(
        task_worker,
        "_repair_ohlcv_integrity_day",
        lambda **_kwargs: {"validation": {"status": "source_gap"}},
    )

    task_worker._run_ohlcv_integrity_repair_all(job_path, payload)

    saved = json.loads(job_path.read_text())
    assert saved["result"]["repaired"] == 0
    assert saved["result"]["source_gaps"] == 1


def test_integrity_repair_all_filters_exact_coin_scope(monkeypatch, tmp_path: Path) -> None:
    """A grouped coin repair does not process damaged days from another coin."""
    job_path = tmp_path / "repair-coin.json"
    payload = {"exchange": "bybit", "coin": "KORU_USDT:USDT"}
    _write_job(job_path, "ohlcv_integrity_repair_all", payload)
    _install_direct_job_updates(monkeypatch)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda *_args: None)
    monkeypatch.setattr(
        task_worker,
        "list_integrity_issues",
        lambda **_kwargs: {
            "rows": [
                {"coin": "KORU_USDT:USDT", "day": "2026-07-15", "market_status": "available"},
                {"coin": "BTC_USDT:USDT", "day": "2026-07-15", "market_status": "available"},
            ]
        },
    )
    repaired = []
    monkeypatch.setattr(
        task_worker,
        "_repair_ohlcv_integrity_day",
        lambda **kwargs: repaired.append(kwargs) or {"validation": {"status": "valid"}},
    )

    task_worker._run_ohlcv_integrity_repair_all(job_path, payload)

    saved = json.loads(job_path.read_text())
    assert [item["storage_coin"] for item in repaired] == ["KORU_USDT:USDT"]
    assert saved["result"]["coin"] == "KORU_USDT:USDT"
    assert saved["result"]["total"] == 1


def test_removed_coin_delete_records_result(monkeypatch, tmp_path: Path) -> None:
    """Removed-coin deletion stores its audited file and catalog counts."""
    job_path = tmp_path / "remove.json"
    payload = {"exchange": "bybit", "coin": "OLD_USDT:USDT"}
    _write_job(job_path, "ohlcv_removed_coin_delete", payload)
    _install_direct_job_updates(monkeypatch)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda *_args: None)
    monkeypatch.setattr(
        task_worker,
        "remove_removed_coin_data",
        lambda **_kwargs: {"files": 12, "catalog_rows": 10, "coin": "OLD_USDT:USDT"},
    )

    task_worker._run_ohlcv_removed_coin_delete(job_path, payload)

    saved = json.loads(job_path.read_text())
    assert saved["result"]["files"] == 12
    assert saved["result"]["catalog_rows"] == 10
    assert saved["progress"]["stage"] == "done"


def test_removed_coins_batch_delete_continues_and_records_partial_result(monkeypatch, tmp_path: Path) -> None:
    """One stale market does not prevent other confirmed unavailable markets from being removed."""
    job_path = tmp_path / "remove-batch.json"
    payload = {"exchange": "bybit", "coins": ["GOOD_USDT:USDT", "STALE_USDT:USDT"]}
    _write_job(job_path, "ohlcv_removed_coins_delete", payload)
    _install_direct_job_updates(monkeypatch)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda *_args: None)

    def remove(*, exchange, coin):
        if coin.startswith("STALE"):
            raise ValueError("market is active")
        return {"files": 12, "bytes": 500, "catalog_rows": 10, "coin": coin}

    monkeypatch.setattr(task_worker, "remove_removed_coin_data", remove)

    with pytest.raises(RuntimeError, match="partially with 1 failed"):
        task_worker._run_ohlcv_removed_coins_delete(job_path, payload)

    saved = json.loads(job_path.read_text())
    assert saved["result"]["removed"] == 1
    assert saved["result"]["failed"] == 1
    assert saved["result"]["files"] == 12
    assert saved["result"]["bytes"] == 500
    assert saved["result"]["failures"] == [{"coin": "STALE_USDT:USDT", "error": "market is active"}]


def test_checksum_publish_uses_snapshot_and_selected_archive(monkeypatch, tmp_path: Path) -> None:
    """Publish jobs create a transient snapshot and pass only its path and archive name."""
    job_path = tmp_path / "publish.json"
    _write_job(job_path, "ohlcv_checksum_publish", {"archive": "mine"})
    _install_direct_job_updates(monkeypatch)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda *_args: None)
    monkeypatch.setattr(task_worker, "get_market_data_root_dir", lambda: tmp_path)
    monkeypatch.setattr(task_worker, "_require_checksum_publish_ready", lambda _path: None)
    published = []

    def snapshot(*, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"snapshot")
        return {"path": str(output_path), "bytes": 8, "sha256": "a" * 64}

    monkeypatch.setattr(task_worker, "create_gzip_snapshot", snapshot)
    monkeypatch.setattr(
        task_worker,
        "publish_release_asset",
        lambda **kwargs: published.append(kwargs) or {"repository": "owner/archive"},
    )

    task_worker._run_ohlcv_checksum_publish(job_path, {"archive": "mine"})

    saved = json.loads(job_path.read_text())
    assert published[0]["archive_name"] == "mine"
    assert published[0]["asset_path"].name == "checksums.sqlite.gz"
    assert not published[0]["asset_path"].exists()
    assert saved["progress"]["stage"] == "done"


def test_checksum_reference_downloads_anonymously_then_compares(monkeypatch, tmp_path: Path) -> None:
    """Reference jobs resolve a public URL and compare the validated installed database."""
    job_path = tmp_path / "reference.json"
    _write_job(job_path, "ohlcv_checksum_reference", {"archive": "community"})
    _install_direct_job_updates(monkeypatch)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda *_args: None)
    reference_path = tmp_path / "reference.sqlite"
    monkeypatch.setattr(task_worker, "release_asset_url", lambda name: f"https://github.com/owner/{name}/asset")
    monkeypatch.setattr(task_worker, "install_reference_snapshot", lambda **kwargs: {"source": kwargs["url"]})
    monkeypatch.setattr(task_worker, "reference_database_path", lambda: reference_path)
    monkeypatch.setattr(
        task_worker,
        "compare_catalogs_readonly",
        lambda **kwargs: {"counts": {"mismatch": 0}, "reference_path": str(kwargs["reference_path"])},
    )

    task_worker._run_ohlcv_checksum_reference(job_path, {"archive": "community"})

    saved = json.loads(job_path.read_text())
    assert saved["result"]["reference"]["source"] == "https://github.com/owner/community/asset"
    assert saved["result"]["comparison"]["reference_path"] == str(reference_path)
