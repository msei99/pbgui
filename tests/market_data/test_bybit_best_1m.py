"""Regression tests for safe Bybit latest-1m refreshes."""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from types import SimpleNamespace

import bybit_best_1m as bybit


class _FixedDateTime(datetime):
    """Freeze the latest refresh window for deterministic pagination tests."""

    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 1, 3, 0, 30, tzinfo=timezone.utc)
        return value if tz is not None else value.replace(tzinfo=None)


def _row(ts_ms: int) -> list[float]:
    """Return one CCXT-shaped OHLCV row."""
    return [ts_ms, 1.0, 2.0, 0.5, 1.5, 3.0]


def _install_fake_ccxt(monkeypatch, fetch_ohlcv) -> None:
    """Install a minimal CCXT Bybit client around the supplied fetch method."""
    exchange = SimpleNamespace(fetch_ohlcv=fetch_ohlcv)
    monkeypatch.setitem(sys.modules, "ccxt", SimpleNamespace(bybit=lambda _config: exchange))


def _prepare_latest_test(monkeypatch, tmp_path) -> list[tuple[str, int, bool]]:
    """Patch storage and logging while recording day writes."""
    writes: list[tuple[str, int, bool]] = []
    monkeypatch.setattr(bybit, "datetime", _FixedDateTime)
    monkeypatch.setattr(bybit, "append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(bybit, "_list_existing_days", lambda _coin: [date(2025, 12, 31)])
    monkeypatch.setattr(bybit, "_bybit_day_path", lambda _coin, day: tmp_path / f"{day}.npz")
    monkeypatch.setattr(
        bybit,
        "_write_candles_for_day",
        lambda _coin, day, candles, overwrite=False, **_kwargs: writes.append(
            (day, len(candles), overwrite)
        )
        or len(candles),
    )
    return writes


def test_latest_refresh_discards_pages_after_fetch_error(monkeypatch, tmp_path) -> None:
    """A later CCXT page failure must not overwrite days with earlier pages."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    start_ms = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)
    calls = 0

    def fetch_ohlcv(_symbol, timeframe, since, limit):
        nonlocal calls
        calls += 1
        if calls == 1:
            return [_row(start_ms + idx * 60_000) for idx in range(limit)]
        raise TimeoutError("temporary Bybit timeout")

    _install_fake_ccxt(monkeypatch, fetch_ohlcv)
    result = bybit.update_latest_bybit_1m_for_coin(coin="BTC", lookback_days=1)

    assert result["result"] == "error"
    assert result["minutes_written"] == 0
    assert writes == []


def test_latest_refresh_rejects_incomplete_closed_day(monkeypatch, tmp_path) -> None:
    """An empty follow-up page must not make a 1000-minute closed day valid."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    start_ms = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)
    calls = 0

    def fetch_ohlcv(_symbol, timeframe, since, limit):
        nonlocal calls
        calls += 1
        if calls == 1:
            return [_row(start_ms + idx * 60_000) for idx in range(limit)]
        return []

    _install_fake_ccxt(monkeypatch, fetch_ohlcv)
    result = bybit.update_latest_bybit_1m_for_coin(coin="BTC", lookback_days=1)

    assert result["result"] == "error"
    assert "ends at minute 999" in result["error"]
    assert result["minutes_written"] == 0
    assert writes == []


def test_latest_refresh_verifies_closed_day_before_writing(monkeypatch, tmp_path) -> None:
    """A complete prior day and partial current day are written after validation."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    last_available_ms = int(datetime(2026, 1, 3, 0, 30, tzinfo=timezone.utc).timestamp() * 1000)

    def fetch_ohlcv(_symbol, timeframe, since, limit):
        if since > last_available_ms:
            return []
        count = min(limit, ((last_available_ms - since) // 60_000) + 1)
        return [_row(since + idx * 60_000) for idx in range(count)]

    _install_fake_ccxt(monkeypatch, fetch_ohlcv)
    result = bybit.update_latest_bybit_1m_for_coin(coin="BTC", lookback_days=1)

    assert result["result"] == "ok"
    assert result["closed_days_checked"] == 1
    assert writes == [
        ("2026-01-02", 1440, True),
        ("2026-01-03", 31, True),
    ]


def test_overwrite_replaces_source_index(monkeypatch, tmp_path) -> None:
    """An NPZ overwrite must replace rather than merge source coverage."""
    replace_calls = []
    update_calls = []
    monkeypatch.setattr(bybit, "_bybit_day_path", lambda _coin, _day: tmp_path / "day.npz")
    monkeypatch.setattr(bybit, "_write_day_npz", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        bybit, "replace_source_index_for_day", lambda **kwargs: replace_calls.append(kwargs)
    )
    monkeypatch.setattr(
        bybit, "update_source_index_for_day", lambda **kwargs: update_calls.append(kwargs)
    )

    candle = {"t": 1, "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 1.0}
    bybit._write_candles_for_day("BTC", "2026-01-02", {0: candle}, overwrite=True)

    assert len(replace_calls) == 1
    assert replace_calls[0]["minute_indices"] == [0]
    assert update_calls == []


def test_current_refresh_fetches_only_current_utc_day(monkeypatch, tmp_path) -> None:
    """Hourly Bybit refresh must no longer fetch completed lookback days."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    current_start = int(datetime(2026, 1, 3, tzinfo=timezone.utc).timestamp() * 1000)
    requested_since = []

    def fetch_ohlcv(_symbol, timeframe, since, limit):
        requested_since.append(since)
        if since > current_start + 30 * 60_000:
            return []
        count = min(limit, ((current_start + 30 * 60_000 - since) // 60_000) + 1)
        return [_row(since + idx * 60_000) for idx in range(count)]

    _install_fake_ccxt(monkeypatch, fetch_ohlcv)
    result = bybit.update_current_bybit_1m_for_coin(coin="BTC", now_utc=_FixedDateTime.now(timezone.utc))

    assert result["result"] == "ok"
    assert requested_since[0] == current_start
    assert writes == [("2026-01-03", 31, True)]


def test_current_day_overwrite_preserves_unfetched_existing_minutes(monkeypatch, tmp_path) -> None:
    """A short current-day response must not truncate newer local candles."""
    day_path = tmp_path / "2026-01-03.npz"
    day_path.touch()
    existing = {
        index: {"t": index * 60_000, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 3.0}
        for index in range(1201)
    }
    fetched = {index: dict(existing[index]) for index in range(1000)}
    written = {}
    monkeypatch.setattr(bybit, "_bybit_day_path", lambda *_args: day_path)
    monkeypatch.setattr(bybit, "_read_day_npz", lambda *_args, **_kwargs: dict(existing))
    monkeypatch.setattr(bybit, "_write_day_npz", lambda _path, candles: written.update(candles))
    monkeypatch.setattr(bybit, "replace_source_index_for_day", lambda **_kwargs: None)

    bybit._write_candles_for_day(
        "BTC",
        "2026-01-03",
        fetched,
        overwrite=True,
        preserve_existing=True,
    )

    assert len(written) == 1201
    assert 1200 in written


def test_current_refresh_rejects_invalid_values_before_merge(monkeypatch, tmp_path) -> None:
    """Malformed current candles cannot overwrite valid overlapping minutes."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    current_start = int(datetime(2026, 1, 3, tzinfo=timezone.utc).timestamp() * 1000)

    def fetch_ohlcv(_symbol, timeframe, since, limit):
        if since != current_start:
            return []
        row = _row(current_start)
        row[5] = -1.0
        return [row]

    _install_fake_ccxt(monkeypatch, fetch_ohlcv)
    result = bybit.update_current_bybit_1m_for_coin(coin="BTC", now_utc=_FixedDateTime.now(timezone.utc))

    assert result["result"] == "error"
    assert "negative volume" in result["error"]
    assert writes == []


def test_candle_validation_rejects_float32_overflow() -> None:
    """Values that serialize to float32 infinity are rejected before writing."""
    candle = {"t": 1, "o": 1e100, "h": 1e100, "l": 1.0, "c": 1e100, "v": 1.0}

    assert "float32 storage range" in bybit._validate_candle_values({0: candle})


def test_candle_validation_ignores_open_envelope_but_rejects_close_bounds() -> None:
    """Bybit finalization follows Passivbot's HLCV representation."""
    open_anomaly = {"t": 1, "o": 9.0, "h": 11.0, "l": 10.0, "c": 10.5, "v": 1.0}
    close_anomaly = {"t": 1, "o": 10.0, "h": 11.0, "l": 10.0, "c": 9.0, "v": 1.0}

    assert bybit._validate_candle_values({0: open_anomaly}) is None
    assert "invalid HLC bounds" in bybit._validate_candle_values({0: close_anomaly})


def test_exact_finalizer_rejects_partial_day(monkeypatch, tmp_path) -> None:
    """Daily finalization must reject the exact 1000-candle tail-gap pattern."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    day_start = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)
    calls = 0

    def fetch_ohlcv(_symbol, timeframe, since, limit):
        nonlocal calls
        calls += 1
        if calls == 1:
            return [_row(day_start + idx * 60_000) for idx in range(1000)]
        return []

    _install_fake_ccxt(monkeypatch, fetch_ohlcv)
    result = bybit.finalize_bybit_1m_day_for_coin(coin="BTC", day="2026-01-02")

    assert result["result"] == "error"
    assert "ends at minute 999" in result["error"]
    assert writes == []


def test_exact_finalizer_writes_complete_day(monkeypatch, tmp_path) -> None:
    """Daily finalization writes and revalidates one complete closed day."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    day_start = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)
    captured = {}

    def fetch_ohlcv(_symbol, timeframe, since, limit):
        end = day_start + 1440 * 60_000
        if since >= end:
            return []
        count = min(limit, (end - since) // 60_000)
        return [_row(since + idx * 60_000) for idx in range(count)]

    def write(_coin, day, candles, overwrite=False, **_kwargs):
        captured.update(candles)
        writes.append((day, len(candles), overwrite))
        return len(candles)

    monkeypatch.setattr(bybit, "_write_candles_for_day", write)
    monkeypatch.setattr(bybit, "_read_day_npz", lambda *_args, **_kwargs: dict(captured))
    _install_fake_ccxt(monkeypatch, fetch_ohlcv)
    result = bybit.finalize_bybit_1m_day_for_coin(coin="BTC", day="2026-01-02")

    assert result["result"] == "ok"
    assert result["minutes_written"] == 1440
    assert writes == [("2026-01-02", 1440, True)]


def test_exact_finalizer_rejects_invalid_values_before_write(monkeypatch, tmp_path) -> None:
    """Invalid OHLCV values cannot replace an existing closed day."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    day_start = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)

    def fetch_ohlcv(_symbol, timeframe, since, limit):
        end = day_start + 1440 * 60_000
        if since >= end:
            return []
        count = min(limit, (end - since) // 60_000)
        rows = [_row(since + idx * 60_000) for idx in range(count)]
        if since == day_start:
            rows[10][5] = -1.0
        return rows

    _install_fake_ccxt(monkeypatch, fetch_ohlcv)
    result = bybit.finalize_bybit_1m_day_for_coin(coin="BTC", day="2026-01-02")

    assert result["result"] == "error"
    assert "negative volume" in result["error"]
    assert writes == []


def test_exact_finalizer_skips_verified_pre_inception_day(monkeypatch, tmp_path) -> None:
    """A verified future inception does not block daily publication as a failed day."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    monkeypatch.setattr(bybit, "_list_existing_days", lambda _coin: [])
    monkeypatch.setattr(bybit, "_probe_inception_date_strict", lambda *_args, **_kwargs: date(2026, 1, 3))
    _install_fake_ccxt(monkeypatch, lambda *_args, **_kwargs: [])

    result = bybit.finalize_bybit_1m_day_for_coin(coin="NEW", day="2026-01-02")

    assert result["result"] == "not_applicable"
    assert result["inception_day"] == "2026-01-03"
    assert writes == []


def test_strict_inception_probe_uses_exact_current_market_launch_time(tmp_path) -> None:
    """Current Bybit instrument metadata proves a reused symbol's new inception."""
    markets_path = tmp_path / "ccxt_markets.json"
    launch_ms = int(datetime(2026, 7, 15, 10, 34, 13, tzinfo=timezone.utc).timestamp() * 1000)
    markets_path.write_text(
        json.dumps(
            {
                "KORU/USDT:USDT": {
                    "id": "KORUUSDT",
                    "quote": "USDT",
                    "swap": True,
                    "linear": True,
                    "active": True,
                    "created": launch_ms,
                    "info": {"symbol": "KORUUSDT", "launchTime": str(launch_ms), "status": "Trading"},
                }
            }
        ),
        encoding="utf-8",
    )

    result = bybit._probe_inception_date_strict("KORU", markets_path=markets_path)

    assert result == date(2026, 7, 15)


def test_targeted_finalizer_verifies_inception_despite_older_local_files(monkeypatch, tmp_path) -> None:
    """Damaged older files cannot prevent targeted inception-day validation."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    day_start = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)
    captured = {}
    calls = 0

    def fetch_ohlcv(_symbol, timeframe, since, limit):
        nonlocal calls
        calls += 1
        if calls > 1:
            return []
        return [_row(day_start + idx * 60_000) for idx in range(500, 1440)]

    def write(_coin, day, candles, overwrite=False, **_kwargs):
        captured.update(candles)
        writes.append((day, len(candles), overwrite))
        return len(candles)

    monkeypatch.setattr(bybit, "_list_existing_days", lambda _coin: [date(2025, 12, 1)])
    monkeypatch.setattr(bybit, "_probe_inception_date_strict", lambda *_args, **_kwargs: date(2026, 1, 2))
    monkeypatch.setattr(bybit, "_write_candles_for_day", write)
    monkeypatch.setattr(bybit, "_read_day_npz", lambda *_args, **_kwargs: dict(captured))
    _install_fake_ccxt(monkeypatch, fetch_ohlcv)

    result = bybit.finalize_bybit_1m_day_for_coin(
        coin="NEW",
        day="2026-01-02",
        verify_inception_independently=True,
    )

    assert result["result"] == "ok"
    assert result["minutes_written"] == 940
    assert writes == [("2026-01-02", 940, True)]
