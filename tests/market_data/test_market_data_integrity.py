"""Tests for the OHLCV integrity catalog and canonical daily hashes."""

from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
import sqlite3
from pathlib import Path

import numpy as np
import pytest

import market_data_integrity as integrity


DTYPE = np.dtype(
    [("ts", "i8"), ("o", "f4"), ("h", "f4"), ("l", "f4"), ("c", "f4"), ("bv", "f4")]
)


def _write_day(path: Path, day: str, indices: list[int]) -> Path:
    """Write a deterministic structured daily NPZ for selected minute indices."""
    day_obj = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_ms = int(day_obj.timestamp() * 1000)
    rows = np.empty(len(indices), dtype=DTYPE)
    rows["ts"] = [start_ms + index * 60_000 for index in indices]
    rows["o"] = 10.0
    rows["h"] = 11.0
    rows["l"] = 9.0
    rows["c"] = 10.5
    rows["bv"] = 1.0
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(path, candles=rows)
    return path


def _write_bybit_mapping(path: Path, *, active: bool) -> Path:
    """Write one deterministic Bybit USDT perpetual mapping row."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps([
            {
                "symbol": "OLDUSDT",
                "quote": "USDT",
                "swap": True,
                "linear": True,
                "active": active,
            }
        ]),
        encoding="utf-8",
    )
    return path


def test_validate_complete_day_and_canonical_hash(tmp_path: Path) -> None:
    """Complete days are valid and independent files hash identically."""
    first = _write_day(tmp_path / "a" / "2026-01-02.npz", "2026-01-02", list(range(1440)))
    second = _write_day(tmp_path / "b" / "2026-01-02.npz", "2026-01-02", list(range(1440)))

    first_result = integrity.validate_daily_npz(first, "2026-01-02")
    second_result = integrity.validate_daily_npz(second, "2026-01-02")

    assert first_result.status == "valid"
    assert first_result.candles == 1440
    assert first_result.missing_minutes == 0
    assert first_result.sha256 == second_result.sha256


def test_validate_ignores_open_envelope_but_enforces_hlc(tmp_path: Path) -> None:
    """Open anomalies are accepted while Passivbot-relevant close bounds remain strict."""
    path = _write_day(tmp_path / "2026-01-02.npz", "2026-01-02", list(range(1440)))
    with np.load(path, allow_pickle=False) as data:
        candles = np.array(data["candles"], copy=True)
    candles[10]["o"] = candles[10]["l"] - 1.0
    np.savez_compressed(path, candles=candles)
    assert integrity.validate_daily_npz(path, "2026-01-02").status == "valid"

    candles[10]["c"] = candles[10]["l"] - 1.0
    np.savez_compressed(path, candles=candles)
    result = integrity.validate_daily_npz(path, "2026-01-02")
    assert result.status == "invalid"
    assert result.error == "invalid HLC bounds"


def test_hyperliquid_fallback_normalization_changes_only_envelope(monkeypatch, tmp_path: Path) -> None:
    """Maintenance expands H/L for other-exchange candles and refreshes the catalog."""
    data_root = tmp_path / "hyperliquid"
    path = _write_day(
        data_root / "1m" / "BTC_USDC:USDC" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1440)),
    )
    with np.load(path, allow_pickle=False) as data:
        candles = np.array(data["candles"], copy=True)
    candles[10]["o"] = 10.0
    candles[10]["h"] = 11.0
    candles[10]["l"] = 9.0
    candles[10]["c"] = 8.0
    original_volume = float(candles[10]["bv"])
    np.savez_compressed(path, candles=candles)
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("hyperliquid", db_path=db_path, data_root=data_root)
    monkeypatch.setattr(
        integrity,
        "get_source_codes_for_day",
        lambda **_kwargs: [integrity.SOURCE_CODE_OTHER] * 1440,
    )

    preview = integrity.normalize_hyperliquid_fallback_envelopes(
        db_path=db_path,
        data_root=data_root,
        dry_run=True,
    )
    result = integrity.normalize_hyperliquid_fallback_envelopes(
        db_path=db_path,
        data_root=data_root,
    )

    with np.load(path, allow_pickle=False) as data:
        repaired = data["candles"]
    assert preview["files_changed"] == 1
    assert float(repaired[10]["l"]) == 8.0
    assert float(repaired[10]["h"]) == 11.0
    assert float(repaired[10]["o"]) == 10.0
    assert float(repaired[10]["c"]) == 8.0
    assert float(repaired[10]["bv"]) == original_volume
    assert result["candles_changed"] == 1
    assert result["still_invalid"] == 0
    assert integrity.catalog_summary(exchange="hyperliquid", db_path=db_path)["counts"] == {"valid": 1}


def test_hyperliquid_scoped_fallback_normalization_does_not_require_catalog_row(
    monkeypatch, tmp_path: Path
) -> None:
    """Exact-day repair normalizes its requested file even before catalog visibility."""
    data_root = tmp_path / "hyperliquid"
    path = _write_day(
        data_root / "1m" / "LIT_USDC:USDC" / "2025-12-23.npz",
        "2025-12-23",
        list(range(1440)),
    )
    with np.load(path, allow_pickle=False) as data:
        candles = np.array(data["candles"], copy=True)
    candles[1229]["c"] = candles[1229]["h"] + 1.0
    np.savez_compressed(path, candles=candles)
    monkeypatch.setattr(
        integrity,
        "get_source_codes_for_day",
        lambda **_kwargs: [integrity.SOURCE_CODE_OTHER] * 1440,
    )

    result = integrity.normalize_hyperliquid_fallback_envelopes(
        coin="LIT_USDC:USDC",
        day="2025-12-23",
        db_path=tmp_path / "checksums.sqlite",
        data_root=data_root,
    )

    assert result["candidates"] == 1
    assert result["candles_changed"] == 1
    assert integrity.validate_daily_npz(path, "2025-12-23").valid


def test_validate_expected_boundaries_and_internal_gap(tmp_path: Path) -> None:
    """Only explicit inception/terminal boundaries may be incomplete."""
    inception = _write_day(tmp_path / "inception" / "2026-01-02.npz", "2026-01-02", list(range(500, 1440)))
    terminal = _write_day(tmp_path / "terminal" / "2026-01-02.npz", "2026-01-02", list(range(1000)))
    internal = _write_day(
        tmp_path / "internal" / "2026-01-02.npz",
        "2026-01-02",
        list(range(500)) + list(range(501, 1440)),
    )

    assert integrity.validate_daily_npz(inception, "2026-01-02").status == "invalid"
    inception_result = integrity.validate_daily_npz(
        inception, "2026-01-02", allow_inception_prefix=True
    )
    assert inception_result.status == "inception_partial"
    assert inception_result.missing_minutes == 0
    assert integrity.validate_daily_npz(terminal, "2026-01-02").status == "invalid"
    assert integrity.validate_daily_npz(
        terminal, "2026-01-02", allow_terminal_suffix=True
    ).status == "terminal_partial"
    assert integrity.validate_daily_npz(internal, "2026-01-02").status == "invalid"


def test_verified_binance_source_gap_is_accepted_only_for_exact_market_day(tmp_path: Path) -> None:
    """The immutable BTC launch-day API gap is accepted without weakening other days."""
    indices = [minute for minute in range(1077, 1440) if minute != 1140]
    known_path = _write_day(tmp_path / "2019-09-08.npz", "2019-09-08", indices)
    other_path = _write_day(tmp_path / "2019-09-09.npz", "2019-09-09", indices)
    allowed = integrity.known_source_gap_minutes("binanceusdm", "BTC_USDT:USDT", "2019-09-08")

    known = integrity.validate_daily_npz(
        known_path,
        "2019-09-08",
        allow_inception_prefix=True,
        allowed_source_gap_minutes=allowed,
    )

    assert known.status == "source_gap"
    assert known.missing_minutes == 1
    assert integrity.validate_daily_npz(
        other_path,
        "2019-09-09",
        allow_inception_prefix=True,
    ).status == "invalid"


def test_dynamic_source_gap_accepts_exact_leading_and_trailing_minutes(tmp_path: Path) -> None:
    """Hyperliquid source proof may cover exact boundary gaps without relaxing normal validation."""
    present = list(range(100, 1300))
    path = _write_day(tmp_path / "2026-01-02.npz", "2026-01-02", present)
    missing = set(range(100)) | set(range(1300, 1440))

    accepted = integrity.validate_daily_npz(
        path,
        "2026-01-02",
        allowed_source_gap_minutes=missing,
    )

    assert accepted.status == "source_gap"
    assert accepted.missing_minutes == 240
    assert integrity.validate_daily_npz(path, "2026-01-02").status == "invalid"


def test_missing_source_gap_persists_until_a_file_appears(tmp_path: Path) -> None:
    """A proven absent day survives scans but is strictly revalidated when data appears."""
    data_root = tmp_path / "hyperliquid"
    coin_root = data_root / "1m" / "TEST_USDC:USDC"
    _write_day(coin_root / "2026-01-01.npz", "2026-01-01", list(range(1440)))
    _write_day(coin_root / "2026-01-03.npz", "2026-01-03", list(range(1440)))
    missing_path = coin_root / "2026-01-02.npz"
    db_path = tmp_path / "checksums.sqlite"
    recorded = integrity.record_proven_source_gap(
        exchange="hyperliquid",
        coin="TEST_USDC:USDC",
        day="2026-01-02",
        path=missing_path,
        db_path=db_path,
    )

    integrity.scan_exchange("hyperliquid", db_path=db_path, data_root=data_root)

    assert recorded.status == "source_gap"
    assert integrity.catalog_summary(exchange="hyperliquid", db_path=db_path)["counts"] == {
        "source_gap": 1,
        "valid": 2,
    }
    integrity._validate_reference_database(db_path)

    _write_day(missing_path, "2026-01-02", list(range(1440)))
    integrity.scan_exchange("hyperliquid", db_path=db_path, data_root=data_root)

    assert integrity.catalog_summary(exchange="hyperliquid", db_path=db_path)["counts"] == {"valid": 3}


def test_scan_and_reference_comparison_include_verified_binance_source_gap(tmp_path: Path) -> None:
    """A known source gap remains accepted across rescans and reference comparison."""
    data_root = tmp_path / "binanceusdm"
    indices = [minute for minute in range(1077, 1440) if minute != 1140]
    _write_day(
        data_root / "1m" / "BTC_USDT:USDT" / "2019-09-08.npz",
        "2019-09-08",
        indices,
    )
    db_path = tmp_path / "checksums.sqlite"

    integrity.scan_exchange("binanceusdm", db_path=db_path, data_root=data_root)
    summary = integrity.catalog_summary(exchange="binanceusdm", db_path=db_path)
    comparison = integrity.compare_catalogs_readonly(
        local_path=db_path,
        reference_path=db_path,
        exchange="binanceusdm",
    )

    assert summary["counts"] == {"source_gap": 1}
    assert integrity.list_integrity_issues(exchange="binanceusdm", db_path=db_path)["rows"] == []
    assert comparison["counts"] == {"local_only": 0, "reference_only": 0, "mismatch": 0, "match": 1}


def test_scan_revalidates_cached_invalid_bybit_source_gap(monkeypatch, tmp_path: Path) -> None:
    """Adding an exact known source gap cannot leave an unchanged file cached as damaged."""
    data_root = tmp_path / "bybit"
    indices = [minute for minute in range(1440) if minute != 350]
    _write_day(
        data_root / "1m" / "XTZ_USDT:USDT" / "2021-01-11.npz",
        "2021-01-11",
        indices,
    )
    db_path = tmp_path / "checksums.sqlite"
    gap_lookup = integrity.known_source_gap_minutes
    monkeypatch.setattr(integrity, "known_source_gap_minutes", lambda *_args: frozenset())
    integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)
    assert integrity.catalog_summary(exchange="bybit", db_path=db_path)["counts"] == {"invalid": 1}

    monkeypatch.setattr(integrity, "known_source_gap_minutes", gap_lookup)
    result = integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)

    assert result["files_validated"] == 1
    assert integrity.catalog_summary(exchange="bybit", db_path=db_path)["counts"] == {"source_gap": 1}
    assert integrity.list_integrity_issues(exchange="bybit", db_path=db_path)["rows"] == []


def test_daily_gap_details_separates_inception_prefix_from_internal_gap(tmp_path: Path) -> None:
    """Minute coverage marks a leading earliest-day range separately from real gaps."""
    data_root = tmp_path / "binanceusdm"
    indices = [minute for minute in range(60, 1440) if minute != 120]
    _write_day(
        data_root / "1m" / "BTC_USDT:USDT" / "2019-09-08.npz",
        "2019-09-08",
        indices,
    )
    _write_day(
        data_root / "1m" / "BTC_USDT:USDT" / "2019-09-09.npz",
        "2019-09-09",
        list(range(1440)),
    )
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("binanceusdm", db_path=db_path, data_root=data_root)

    result = integrity.daily_gap_details(
        exchange="binanceusdm",
        coin="BTC_USDT:USDT",
        day="2019-09-08",
        data_root=data_root,
        db_path=db_path,
    )

    assert result["missing_minutes"] == 61
    assert result["damaged_missing_minutes"] == 1
    assert result["first"] == "01:00"
    assert result["last"] == "23:59"
    assert result["earliest_local_day"] is True
    assert result["coverage"][0] == "l"
    assert result["coverage"][60] == "p"
    assert result["coverage"][120] == "i"
    assert len(result["day_context"]) == 15
    assert result["day_context"][7]["selected"] is True
    assert result["day_context"][8]["day"] == "2019-09-09"
    assert result["day_context"][8]["hourly_coverage"] == "p" * 24
    assert result["day_context"][8]["candles"] == 1440
    issues = integrity.list_integrity_issues(exchange="binanceusdm", db_path=db_path)
    assert issues["rows"][0]["missing_minutes"] == 1
    assert result["ranges"] == [
        {
            "start_minute": 0,
            "end_minute": 59,
            "start": "00:00",
            "end": "00:59",
            "minutes": 60,
            "kind": "leading",
            "possible_inception": True,
        },
        {
            "start_minute": 120,
            "end_minute": 120,
            "start": "02:00",
            "end": "02:00",
            "minutes": 1,
            "kind": "internal",
            "possible_inception": False,
        },
    ]


def test_daily_gap_details_rejects_symlinked_coin_directory(tmp_path: Path) -> None:
    """Detail reads cannot escape the selected exchange dataset through a symlink."""
    data_root = tmp_path / "binanceusdm"
    outside = tmp_path / "outside"
    outside.mkdir()
    (data_root / "1m").mkdir(parents=True)
    (data_root / "1m" / "BTC_USDT:USDT").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="detail path"):
        integrity.daily_gap_details(
            exchange="binanceusdm",
            coin="BTC_USDT:USDT",
            day="2019-09-08",
            data_root=data_root,
            db_path=tmp_path / "checksums.sqlite",
        )


def test_scan_records_internal_tail_gap_and_completion_marker(tmp_path: Path) -> None:
    """Initial scan catalogs the 1000-candle overwrite pattern as invalid."""
    data_root = tmp_path / "bybit"
    coin_root = data_root / "1m" / "BTC_USDT:USDT"
    _write_day(coin_root / "2026-01-01.npz", "2026-01-01", list(range(1440)))
    _write_day(coin_root / "2026-01-02.npz", "2026-01-02", list(range(1000)))
    _write_day(coin_root / "2026-01-03.npz", "2026-01-03", list(range(1440)))
    db_path = tmp_path / "checksums.sqlite"

    result = integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)
    issues = integrity.list_integrity_issues(exchange="bybit", db_path=db_path)

    assert result["files_scanned"] == 3
    assert result["invalid_days"] == 1
    assert integrity.initial_scan_required("bybit", db_path=db_path) is False
    assert issues["total"] == 1
    assert issues["rows"][0]["coin"] == "BTC_USDT:USDT"
    assert issues["rows"][0]["day"] == "2026-01-02"
    assert issues["rows"][0]["missing_minutes"] == 440
    assert list(data_root.rglob("*.npz.lock")) == []


@pytest.mark.parametrize("exchange", integrity.SUPPORTED_EXCHANGES)
def test_scan_supports_each_integrity_exchange(exchange: str, tmp_path: Path) -> None:
    """Every supported storage exchange receives an isolated catalog namespace."""
    data_root = tmp_path / exchange
    quote = "USDC" if exchange == "hyperliquid" else "USDT"
    coin = f"BTC_{quote}:{quote}"
    _write_day(data_root / "1m" / coin / "2026-01-02.npz", "2026-01-02", list(range(1440)))
    db_path = tmp_path / "checksums.sqlite"

    result = integrity.scan_exchange(exchange, db_path=db_path, data_root=data_root)
    summary = integrity.catalog_summary(exchange=exchange, db_path=db_path)

    assert result["files_scanned"] == 1
    assert summary["counts"] == {"valid": 1}
    assert summary["initial_scan_complete"] is True


def test_hyperliquid_scan_excludes_xyz_tradfi_directories(tmp_path: Path) -> None:
    """Phase-one Hyperliquid integrity covers native crypto but not session-based XYZ data."""
    data_root = tmp_path / "hyperliquid"
    _write_day(
        data_root / "1m" / "BTC_USDC:USDC" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1440)),
    )
    _write_day(
        data_root / "1m" / "XYZ-AAPL_USDC:USDC" / "2026-01-02.npz",
        "2026-01-02",
        list(range(390)),
    )
    db_path = tmp_path / "checksums.sqlite"

    result = integrity.scan_exchange("hyperliquid", db_path=db_path, data_root=data_root)

    assert result["files_scanned"] == 1
    with sqlite3.connect(db_path) as conn:
        coins = {str(row[0]) for row in conn.execute("SELECT DISTINCT coin FROM daily_checksums")}
    assert coins == {"BTC_USDC:USDC"}


def test_all_exchange_issues_include_market_status_and_repair(monkeypatch, tmp_path: Path) -> None:
    """Every scanned exchange classifies availability and exposes repair capability."""
    data_root = tmp_path / "okx"
    _write_day(
        data_root / "1m" / "TEST_USDT:USDT" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1000)),
    )
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("okx", db_path=db_path, data_root=data_root)
    monkeypatch.setattr(
        integrity,
        "storage_market_status",
        lambda _exchange, _coin: {"status": "available", "reason": "active"},
    )

    issues = integrity.list_integrity_issues(exchange="okx", db_path=db_path)

    assert issues["repair_supported"] is True
    assert issues["rows"][0]["market_status"] == "available"
    assert issues["rows"][0]["repair_supported"] is True


def test_integrity_repair_resolves_colliding_cat_storage_exactly(tmp_path: Path) -> None:
    """Integrity repair maps CAT storage directories back to distinct downloader coins."""
    mapping_path = tmp_path / "mapping.json"
    mapping_path.write_text(json.dumps([
        {
            "symbol": "1000CATUSDT",
            "ccxt_symbol": "1000CAT/USDT:USDT",
            "base": "1000CAT",
            "coin": "CAT",
            "quote": "USDT",
            "swap": True,
            "linear": True,
        },
        {
            "symbol": "CATUSDT",
            "ccxt_symbol": "CAT/USDT:USDT",
            "base": "CAT",
            "coin": "CAT",
            "quote": "USDT",
            "swap": True,
            "linear": True,
        },
    ]), encoding="utf-8")

    assert integrity.repair_coin_from_storage(
        "binanceusdm", "1000CAT_USDT:USDT", mapping_path=mapping_path
    ) == "1000CAT"
    assert integrity.repair_coin_from_storage(
        "binanceusdm", "CAT_USDT:USDT", mapping_path=mapping_path
    ) == "CAT"


@pytest.mark.parametrize("exchange", ["binanceusdm", "okx", "bitget", "hyperliquid"])
def test_removed_coin_mutation_remains_bybit_only(exchange: str, tmp_path: Path) -> None:
    """Expanding scan support does not expand destructive removal capability."""
    with pytest.raises(ValueError, match="only for Bybit"):
        integrity.removed_coin_data_preview(
            exchange=exchange,
            coin="BTC_USDT:USDT",
            data_root=tmp_path / exchange,
            mapping_path=tmp_path / "mapping.json",
        )


def test_scan_accepts_earliest_contiguous_suffix_but_not_terminal_prefix(tmp_path: Path) -> None:
    """Read-only discovery accepts a local inception suffix but not an unproven terminal prefix."""
    data_root = tmp_path / "bybit"
    coin_root = data_root / "1m" / "BTC_USDT:USDT"
    _write_day(coin_root / "2026-01-01.npz", "2026-01-01", list(range(500, 1440)))
    _write_day(coin_root / "2026-01-02.npz", "2026-01-02", list(range(1440)))
    _write_day(coin_root / "2026-01-03.npz", "2026-01-03", list(range(1000)))
    db_path = tmp_path / "checksums.sqlite"

    integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)
    issues = integrity.list_integrity_issues(exchange="bybit", db_path=db_path)

    assert {(row["day"], row["missing_minutes"]) for row in issues["rows"]} == {
        ("2026-01-03", 440),
    }
    assert integrity.catalog_summary(exchange="bybit", db_path=db_path)["counts"] == {
        "inception_partial": 1,
        "invalid": 1,
        "valid": 1,
    }


def test_rescan_removes_catalog_rows_for_deleted_files(tmp_path: Path) -> None:
    """A completed rescan must not publish stale checksums for removed files."""
    data_root = tmp_path / "bybit"
    day_path = _write_day(
        data_root / "1m" / "BTC_USDT:USDT" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1440)),
    )
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)
    day_path.unlink()

    integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)

    assert integrity.catalog_summary(exchange="bybit", db_path=db_path)["counts"] == {}


def test_rescan_reuses_unchanged_catalog_rows(monkeypatch, tmp_path: Path) -> None:
    """An unchanged daily file reuses its catalog hash without reopening the NPZ."""
    data_root = tmp_path / "bybit"
    _write_day(
        data_root / "1m" / "BTC_USDT:USDT" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1440)),
    )
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)
    monkeypatch.setattr(
        integrity,
        "validate_daily_npz",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unchanged NPZ reopened")),
    )

    result = integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)

    assert result["files_scanned"] == 1
    assert result["files_reused"] == 1
    assert result["files_validated"] == 0
    assert result["accepted_days"] == 1


def test_rescan_revalidates_changed_daily_file(monkeypatch, tmp_path: Path) -> None:
    """A changed file is reopened and replaces its previous catalog validation."""
    data_root = tmp_path / "bybit"
    day_path = _write_day(
        data_root / "1m" / "BTC_USDT:USDT" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1440)),
    )
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)
    _write_day(day_path, "2026-01-02", list(range(1000)))
    original_validate = integrity.validate_daily_npz
    calls: list[str] = []

    def recording_validate(path: Path, day: str | date, **kwargs):
        """Record the changed file before applying normal validation."""
        calls.append(str(day))
        return original_validate(path, day, **kwargs)

    monkeypatch.setattr(integrity, "validate_daily_npz", recording_validate)

    result = integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)

    assert calls == ["2026-01-02"]
    assert result["files_reused"] == 0
    assert result["files_validated"] == 1
    assert result["invalid_days"] == 1


def test_rescan_revalidates_cached_inception_when_earlier_file_appears(tmp_path: Path) -> None:
    """Adding an earlier day cannot leave the old first day accepted as inception."""
    data_root = tmp_path / "bybit"
    coin_root = data_root / "1m" / "BTC_USDT:USDT"
    _write_day(coin_root / "2026-01-02.npz", "2026-01-02", list(range(500, 1440)))
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)
    _write_day(coin_root / "2026-01-01.npz", "2026-01-01", list(range(1440)))

    result = integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)

    assert result["files_reused"] == 0
    assert result["files_validated"] == 2
    assert integrity.catalog_summary(exchange="bybit", db_path=db_path)["counts"] == {
        "invalid": 1,
        "valid": 1,
    }


def test_cancelled_rescan_clears_previous_completion_marker(tmp_path: Path) -> None:
    """A partial rescan cannot leave the previous generation publishable."""
    data_root = tmp_path / "bybit"
    _write_day(
        data_root / "1m" / "BTC_USDT:USDT" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1440)),
    )
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)

    with pytest.raises(RuntimeError, match="cancelled"):
        integrity.scan_exchange(
            "bybit",
            db_path=db_path,
            data_root=data_root,
            stop_check=lambda: True,
        )

    assert integrity.initial_scan_required("bybit", db_path=db_path) is True


def test_discovery_failure_clears_previous_completion_marker(monkeypatch, tmp_path: Path) -> None:
    """Even a failure before file enumeration leaves the exchange unpublished."""
    data_root = tmp_path / "bybit"
    _write_day(
        data_root / "1m" / "BTC_USDT:USDT" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1440)),
    )
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)
    original_iterdir = Path.iterdir

    def failing_iterdir(path: Path):
        if path == data_root / "1m":
            raise OSError("discovery failed")
        return original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", failing_iterdir)

    with pytest.raises(OSError, match="discovery failed"):
        integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)

    assert integrity.initial_scan_required("bybit", db_path=db_path) is True


def test_inventory_deletion_invalidation_blocks_publish_until_rescan(tmp_path: Path) -> None:
    """Catalog rows and scan marker are cleared before managed files are deleted."""
    data_root = tmp_path / "bybit"
    day_path = _write_day(
        data_root / "1m" / "BTC_USDT:USDT" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1440)),
    )
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)

    removed = integrity.invalidate_catalog_for_deletion(
        exchange="bybit",
        coins=["BTC_USDT:USDT"],
        db_path=db_path,
    )

    assert removed == 1
    assert day_path.is_file()
    assert integrity.initial_scan_required("bybit", db_path=db_path) is True
    assert integrity.catalog_summary(exchange="bybit", db_path=db_path)["counts"] == {}


def test_remove_catalog_before_day_is_scoped_and_preserves_scan_marker(tmp_path: Path) -> None:
    """Verified inception cleanup removes only earlier rows for the exact market."""
    data_root = tmp_path / "bybit"
    for coin, day in (
        ("KORU_USDT:USDT", "2026-01-01"),
        ("KORU_USDT:USDT", "2026-01-02"),
        ("KORU_USDT:USDT", "2026-01-03"),
        ("BTC_USDT:USDT", "2026-01-01"),
    ):
        _write_day(data_root / "1m" / coin / f"{day}.npz", day, list(range(1440)))
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)

    removed = integrity.remove_catalog_before_day(
        exchange="bybit",
        coin="KORU_USDT:USDT",
        before_day="2026-01-03",
        db_path=db_path,
    )

    assert removed == 2
    assert integrity.initial_scan_required("bybit", db_path=db_path) is False
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT coin, day FROM daily_checksums ORDER BY coin, day"
        ).fetchall()
    assert rows == [
        ("BTC_USDT:USDT", "2026-01-01"),
        ("KORU_USDT:USDT", "2026-01-03"),
    ]


def test_bybit_storage_market_status_distinguishes_active_removed_and_unknown(tmp_path: Path) -> None:
    """Removed-coin deletion is enabled only from a valid current mapping."""
    mapping_path = _write_bybit_mapping(tmp_path / "mapping.json", active=True)

    assert integrity.bybit_storage_market_status("OLD_USDT:USDT", mapping_path=mapping_path)["status"] == "available"
    assert integrity.bybit_storage_market_status("MISSING_USDT:USDT", mapping_path=mapping_path)["status"] == "removed"
    mapping_path.write_text("not-json", encoding="utf-8")
    assert integrity.bybit_storage_market_status("OLD_USDT:USDT", mapping_path=mapping_path)["status"] == "unknown"


def test_remove_removed_coin_data_deletes_raw_data_and_preserves_scan_marker(tmp_path: Path) -> None:
    """Confirmed removed coins lose PBGui raw data and catalog rows, not scan state."""
    data_root = tmp_path / "bybit"
    day_path = _write_day(
        data_root / "1m" / "OLD_USDT:USDT" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1440)),
    )
    source_index = data_root / "1m_src" / "OLD_USDT:USDT" / "sources.idx"
    source_index.parent.mkdir(parents=True, exist_ok=True)
    source_index.write_bytes(b"source-index")
    mapping_path = _write_bybit_mapping(tmp_path / "mapping.json", active=False)
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("bybit", db_path=db_path, data_root=data_root)

    preview = integrity.removed_coin_data_preview(
        exchange="bybit",
        coin="OLD_USDT:USDT",
        data_root=data_root,
        mapping_path=mapping_path,
    )
    result = integrity.remove_removed_coin_data(
        exchange="bybit",
        coin="OLD_USDT:USDT",
        data_root=data_root,
        mapping_path=mapping_path,
        db_path=db_path,
    )

    assert preview["files"] == 2
    assert result["catalog_rows"] == 1
    assert not day_path.parent.exists()
    assert not source_index.parent.exists()
    assert integrity.initial_scan_required("bybit", db_path=db_path) is False
    assert integrity.catalog_summary(exchange="bybit", db_path=db_path)["counts"] == {}


def test_remove_removed_coin_data_refuses_active_market(tmp_path: Path) -> None:
    """A stale browser cannot delete a market that became active again."""
    mapping_path = _write_bybit_mapping(tmp_path / "mapping.json", active=True)

    with pytest.raises(ValueError, match="active"):
        integrity.remove_removed_coin_data(
            exchange="bybit",
            coin="OLD_USDT:USDT",
            data_root=tmp_path / "bybit",
            mapping_path=mapping_path,
            db_path=tmp_path / "checksums.sqlite",
        )


def test_list_removed_coin_data_includes_valid_local_history(tmp_path: Path) -> None:
    """Removed-market cleanup is offered even when all local days are valid."""
    data_root = tmp_path / "bybit"
    _write_day(
        data_root / "1m" / "OLD_USDT:USDT" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1440)),
    )
    _write_day(
        data_root / "1m" / "OLD_USDT:USDT" / "2026-01-04.npz",
        "2026-01-04",
        list(range(1440)),
    )
    mapping_path = _write_bybit_mapping(tmp_path / "mapping.json", active=False)

    result = integrity.list_removed_coin_data(
        exchange="bybit",
        data_root=data_root,
        mapping_path=mapping_path,
    )

    assert result["total"] == 1
    assert result["rows"][0]["coin"] == "OLD_USDT:USDT"
    assert result["rows"][0]["market_status"] == "removed"
    assert result["rows"][0]["removable"] is True
    assert result["rows"][0]["from_day"] == "2026-01-02"
    assert result["rows"][0]["to_day"] == "2026-01-04"

    preview = integrity.unavailable_coin_data_batch_preview(
        exchange="bybit",
        coins=["OLD_USDT:USDT"],
        data_root=data_root,
        mapping_path=mapping_path,
    )
    assert preview["coins"] == ["OLD_USDT:USDT"]
    assert preview["coin_count"] == 1
    assert preview["files"] == 2
    assert preview["from_day"] == "2026-01-02"
    assert preview["to_day"] == "2026-01-04"


def test_removed_coin_batch_preview_rejects_stale_selection(tmp_path: Path) -> None:
    """A selected market that is no longer removable blocks batch queueing."""
    mapping_path = _write_bybit_mapping(tmp_path / "mapping.json", active=True)

    with pytest.raises(ValueError, match="stale or unsafe"):
        integrity.unavailable_coin_data_batch_preview(
            exchange="bybit",
            coins=["OLD_USDT:USDT"],
            data_root=tmp_path / "bybit",
            mapping_path=mapping_path,
        )


def test_hyperliquid_removed_coin_data_is_listed_as_removable(tmp_path: Path) -> None:
    """Inactive Hyperliquid crypto history uses the common confirmed deletion path."""
    data_root = tmp_path / "hyperliquid"
    _write_day(
        data_root / "1m" / "OLD_USDC:USDC" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1440)),
    )
    _write_day(
        data_root / "1m" / "XYZ-AAPL_USDC:USDC" / "2026-01-02.npz",
        "2026-01-02",
        list(range(390)),
    )
    mapping_path = tmp_path / "mapping.json"
    mapping_path.write_text(
        json.dumps([
            {
                "ccxt_symbol": "OLD/USDC:USDC",
                "coin": "OLD",
                "quote": "USDC",
                "swap": True,
                "linear": True,
                "active": False,
            }
        ]),
        encoding="utf-8",
    )

    result = integrity.list_removed_coin_data(
        exchange="hyperliquid",
        data_root=data_root,
        mapping_path=mapping_path,
    )

    assert result["total"] == 1
    assert result["rows"][0]["coin"] == "OLD_USDC:USDC"
    assert result["rows"][0]["market_status"] == "removed"
    assert result["rows"][0]["removable"] is True


def test_remove_hyperliquid_unavailable_coin_data(tmp_path: Path) -> None:
    """Confirmed non-Bybit deletion removes only the selected exchange coin footprint."""
    data_root = tmp_path / "hyperliquid"
    day_path = _write_day(
        data_root / "1m" / "OLD_USDC:USDC" / "2026-01-02.npz",
        "2026-01-02",
        list(range(1440)),
    )
    mapping_path = tmp_path / "mapping.json"
    mapping_path.write_text(
        json.dumps([{
            "ccxt_symbol": "OLD/USDC:USDC",
            "coin": "OLD",
            "quote": "USDC",
            "swap": True,
            "linear": True,
            "active": False,
        }]),
        encoding="utf-8",
    )
    db_path = tmp_path / "checksums.sqlite"
    integrity.scan_exchange("hyperliquid", db_path=db_path, data_root=data_root)

    result = integrity.remove_removed_coin_data(
        exchange="hyperliquid",
        coin="OLD_USDC:USDC",
        data_root=data_root,
        mapping_path=mapping_path,
        db_path=db_path,
    )

    assert result["catalog_rows"] == 1
    assert not day_path.parent.exists()


def test_catalog_finalization_marker_matches_file_fingerprint(tmp_path: Path) -> None:
    """A finalized row is invalidated when its daily file changes."""
    day_path = _write_day(tmp_path / "2026-01-02.npz", "2026-01-02", list(range(1440)))
    db_path = tmp_path / "checksums.sqlite"
    integrity.record_daily_file(
        exchange="bybit",
        coin="BTC_USDT:USDT",
        day="2026-01-02",
        path=day_path,
        db_path=db_path,
    )

    assert integrity.day_is_finalized(
        exchange="bybit",
        coin="BTC_USDT:USDT",
        day="2026-01-02",
        path=day_path,
        db_path=db_path,
    )
    day_path.touch()
    assert not integrity.day_is_finalized(
        exchange="bybit",
        coin="BTC_USDT:USDT",
        day="2026-01-02",
        path=day_path,
        db_path=db_path,
    )


def test_oldest_unfinalized_day_supports_bounded_restart_catchup(tmp_path: Path) -> None:
    """Catch-up selects an older unfinished day before yesterday."""
    db_path = tmp_path / "checksums.sqlite"
    paths = {}
    for day in ("2026-08-01", "2026-08-02"):
        paths[day] = _write_day(tmp_path / f"{day}.npz", day, list(range(1440)))
    integrity.record_daily_file(
        exchange="bybit",
        coin="BTC_USDT:USDT",
        day="2026-08-01",
        path=paths["2026-08-01"],
        db_path=db_path,
    )
    integrity.record_daily_file(
        exchange="bybit",
        coin="BTC_USDT:USDT",
        day="2026-08-02",
        path=paths["2026-08-02"],
        current_day=True,
        db_path=db_path,
    )

    pending = integrity.oldest_unfinalized_day(
        exchange="bybit",
        coin="BTC_USDT:USDT",
        through_day=date(2026, 8, 3),
        lookback_days=3,
        path_for_day=lambda candidate: tmp_path / f"{candidate.isoformat()}.npz",
        db_path=db_path,
    )

    assert pending == date(2026, 8, 2)


def test_snapshot_and_readonly_compare(tmp_path: Path) -> None:
    """SQLite backup snapshots preserve hashes and compare without writes."""
    day_path = _write_day(tmp_path / "2026-01-02.npz", "2026-01-02", list(range(1440)))
    local_db = tmp_path / "checksums.sqlite"
    integrity.record_daily_file(
        exchange="bybit",
        coin="BTC_USDT:USDT",
        day="2026-01-02",
        path=day_path,
        db_path=local_db,
    )
    snapshot_gz = tmp_path / "reference" / "checksums.sqlite.gz"
    snapshot = integrity.create_gzip_snapshot(db_path=local_db, output_path=snapshot_gz)
    reference_db = tmp_path / "reference" / "checksums.sqlite"
    with __import__("gzip").open(snapshot_gz, "rb") as source:
        reference_db.write_bytes(source.read())

    comparison = integrity.compare_catalogs_readonly(
        local_path=local_db,
        reference_path=reference_db,
    )

    assert snapshot["bytes"] > 0
    assert len(snapshot["sha256"]) == 64
    assert comparison["counts"]["match"] == 1
    assert comparison["differences"] == []


def test_reference_comparison_can_scope_one_exchange(tmp_path: Path) -> None:
    """Selected-exchange comparison excludes valid rows from other exchanges."""
    shared_day = _write_day(tmp_path / "shared" / "2026-01-02.npz", "2026-01-02", list(range(1440)))
    local_okx = _write_day(tmp_path / "local-okx" / "2026-01-03.npz", "2026-01-03", list(range(1440)))
    reference_okx = _write_day(tmp_path / "reference-okx" / "2026-01-04.npz", "2026-01-04", list(range(1440)))
    local_db = tmp_path / "local.sqlite"
    reference_db = tmp_path / "reference.sqlite"
    for db_path in (local_db, reference_db):
        integrity.record_daily_file(
            exchange="bybit",
            coin="BTC_USDT:USDT",
            day="2026-01-02",
            path=shared_day,
            db_path=db_path,
        )
    integrity.record_daily_file(
        exchange="okx",
        coin="BTC_USDT:USDT",
        day="2026-01-03",
        path=local_okx,
        db_path=local_db,
    )
    integrity.record_daily_file(
        exchange="okx",
        coin="BTC_USDT:USDT",
        day="2026-01-04",
        path=reference_okx,
        db_path=reference_db,
    )

    comparison = integrity.compare_catalogs_readonly(
        local_path=local_db,
        reference_path=reference_db,
        exchange="bybit",
    )

    assert comparison["counts"] == {"local_only": 0, "reference_only": 0, "mismatch": 0, "match": 1}
    assert comparison["differences"] == []


def test_reference_install_is_anonymous_validated_and_preserves_last_good(monkeypatch, tmp_path: Path) -> None:
    """Reference refresh installs only valid SQLite gzip data and retains the last good copy."""
    source_root = tmp_path / "source"
    day_path = _write_day(source_root / "day" / "2026-01-02.npz", "2026-01-02", list(range(1440)))
    source_db = source_root / "checksums.sqlite"
    integrity.record_daily_file(
        exchange="bybit",
        coin="BTC_USDT:USDT",
        day="2026-01-02",
        path=day_path,
        db_path=source_db,
    )
    source_gz = source_root / "checksums.sqlite.gz"
    integrity.create_gzip_snapshot(db_path=source_db, output_path=source_gz)
    payloads = [source_gz.read_bytes(), b"not-a-gzip"]

    class Response:
        def __init__(self, payload: bytes):
            self.payload = payload
            self.offset = 0

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return "https://release-assets.githubusercontent.com/download/checksums.sqlite.gz"

        def read(self, size: int):
            chunk = self.payload[self.offset:self.offset + size]
            self.offset += len(chunk)
            return chunk

    monkeypatch.setattr(integrity, "urlopen", lambda *_args, **_kwargs: Response(payloads.pop(0)))
    target_root = tmp_path / "target"
    url = "https://github.com/owner/archive/releases/download/checksums-latest/checksums.sqlite.gz"
    state = integrity.install_reference_snapshot(url=url, root=target_root)
    installed = integrity.reference_database_path(target_root)
    digest_before = hashlib.sha256(installed.read_bytes()).hexdigest()

    assert state["database_bytes"] > 0
    assert integrity.reference_status(target_root)["available"] is True
    with __import__("pytest").raises(Exception):
        integrity.install_reference_snapshot(url=url, root=target_root)
    assert hashlib.sha256(installed.read_bytes()).hexdigest() == digest_before


def test_reference_validation_rejects_semantically_impossible_valid_row(tmp_path: Path) -> None:
    """A public catalog cannot label an empty day as valid with a synthetic hash."""
    day_path = _write_day(tmp_path / "2026-01-02.npz", "2026-01-02", list(range(1440)))
    db_path = tmp_path / "checksums.sqlite"
    integrity.record_daily_file(
        exchange="bybit",
        coin="BTC_USDT:USDT",
        day="2026-01-02",
        path=day_path,
        db_path=db_path,
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE daily_checksums SET candles=0, missing_minutes=1440, first_ts=NULL, last_ts=NULL"
        )

    with __import__("pytest").raises(RuntimeError, match="timestamp bounds|incomplete valid day"):
        integrity._validate_reference_database(db_path)
