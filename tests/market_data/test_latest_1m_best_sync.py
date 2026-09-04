from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json

import hyperliquid_best_1m as hb

from hyperliquid_best_1m import (
    _determine_best_sync_start,
    _determine_stock_perp_improve_start,
    _is_stock_perp_coin,
    _tradfi_ticker_from_hyperliquid_coin,
)


def _recent_weekday() -> date:
    d = date.today() - timedelta(days=1)
    for _ in range(7):
        if hb._default_us_equity_session_utc(d) is not None:
            return d
        d -= timedelta(days=1)
    return date.today()


def test_hyperliquid_exact_day_reads_archived_l2book_hours(monkeypatch, tmp_path) -> None:
    """Integrity repair resolves archived L2Book hours instead of requiring local raw files."""
    archived = tmp_path / "archive" / "BTC_USDC:USDC" / "20260102-01.lz4"
    archived.parent.mkdir(parents=True)
    archived.write_bytes(b"archived")
    day_start = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)
    resolved = []

    def resolve(**kwargs):
        resolved.append(kwargs["hour"])
        return archived if kwargs["hour"] == 1 else None

    monkeypatch.setattr(hb, "_resolve_l2book_hour_path", resolve)
    monkeypatch.setattr(
        hb,
        "iter_hyperliquid_l2book_mid_prices",
        lambda path: iter([(day_start + 62 * 60_000, Decimal("100.5"))]) if path == archived else iter(()),
    )

    result = hb._l2book_minutes_for_day(coin="BTC", day="20260102")

    assert resolved == list(range(24))
    assert result[62]["c"] == 100.5


def test_hyperliquid_source_gap_requires_every_minute_before_donors(monkeypatch, tmp_path) -> None:
    """Current local market metadata bounds source gaps at exact donor inception minutes."""
    coindata = tmp_path / "coindata"
    for exchange, market, info in (
        ("binance", {"id": "TESTUSDT", "created": 1767318000000}, {"onboardDate": "1767318000000"}),
        ("bybit", {"id": "TESTUSDT", "created": 1767324000000}, {"launchTime": "1767324000000"}),
    ):
        path = coindata / exchange / "ccxt_markets.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"TEST/USDT:USDT": {**market, "info": info}}), encoding="utf-8")
    monkeypatch.setattr(hb, "_get_symbol_for_coin", lambda *_args: "TESTUSDT")
    hb._SOURCE_GAP_MARKETS_CACHE.clear()

    before = hb.classify_hyperliquid_pre_donor_gap(
        coin="TEST",
        day="2026-01-02",
        missing_minutes={30},
        coindata_root=coindata,
    )
    covered = hb.classify_hyperliquid_pre_donor_gap(
        coin="TEST",
        day="2026-01-02",
        missing_minutes={120},
        coindata_root=coindata,
    )

    assert before["eligible"] is True
    assert covered["eligible"] is False
    assert "binanceusdm may cover" in covered["reason"]


def test_external_fallback_expands_envelope_after_l2_boundary_adjustment(monkeypatch, tmp_path) -> None:
    """Adjusted fallback Open/Close values remain enclosed by High/Low."""
    target_day = date(2026, 1, 2)
    day_start = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)
    day_path = tmp_path / "2026-01-02.npz"
    day_path.write_bytes(b"existing")
    existing = {
        0: {"t": day_start, "o": 12.0, "h": 12.0, "l": 12.0, "c": 12.0, "v": 1.0},
        2: {"t": day_start + 120_000, "o": 8.0, "h": 8.0, "l": 8.0, "c": 8.0, "v": 1.0},
    }
    written = []

    class FakeExchange:
        def __init__(self, _exchange_id):
            pass

        def fetch_ohlcv(self, _symbol, _market_type, _timeframe, *, limit, since):
            return [[day_start + 60_000, 10.0, 11.0, 9.0, 10.0, 2.0]]

    monkeypatch.setattr(hb, "normalize_hyperliquid_coin", lambda coin: coin)
    monkeypatch.setattr(hb, "normalize_market_data_coin_dir", lambda *_args: "BTC_USDC:USDC")
    monkeypatch.setattr(hb, "_best_day_path", lambda **_kwargs: day_path)
    monkeypatch.setattr(hb, "_read_day_npz", lambda *_args, **_kwargs: dict(existing))
    monkeypatch.setattr(hb, "get_source_codes_for_day", lambda **_kwargs: [hb.SOURCE_CODE_L2BOOK, 0, hb.SOURCE_CODE_L2BOOK] + [0] * 1437)
    monkeypatch.setattr(hb, "_write_day_npz", lambda _path, candles: written.append(candles))
    monkeypatch.setattr(hb, "update_source_index_for_day", lambda **_kwargs: None)
    monkeypatch.setattr(hb, "Exchange", FakeExchange)
    stats = {}

    added = hb._fill_missing_from_exchange_perp_1m(
        exchange_id="binanceusdm",
        symbol="BTC/USDT:USDT",
        coin="BTC",
        start_date=target_day,
        end_date=target_day,
        sleep_s=0.0,
        stats_out=stats,
    )

    fallback = written[0][1]
    assert added == 1
    assert fallback["o"] == 12.0
    assert fallback["c"] == 8.0
    assert fallback["h"] == 12.0
    assert fallback["l"] == 8.0
    assert stats == {"days_requested": 1, "days_completed": 1}


def test_external_fallback_records_fetch_errors_as_incomplete(monkeypatch, tmp_path) -> None:
    """A donor network failure cannot become proof that historical candles are unavailable."""
    day_path = tmp_path / "2026-01-02.npz"
    day_path.write_bytes(b"existing")

    class FailingExchange:
        def __init__(self, _exchange_id):
            pass

        def fetch_ohlcv(self, *_args, **_kwargs):
            raise RuntimeError("network down")

    monkeypatch.setattr(hb, "normalize_hyperliquid_coin", lambda coin: coin)
    monkeypatch.setattr(hb, "normalize_market_data_coin_dir", lambda *_args: "BTC_USDC:USDC")
    monkeypatch.setattr(hb, "_best_day_path", lambda **_kwargs: day_path)
    monkeypatch.setattr(hb, "_read_day_npz", lambda *_args, **_kwargs: {0: {"t": 1}})
    monkeypatch.setattr(hb, "Exchange", FailingExchange)
    monkeypatch.setattr(hb, "append_exchange_download_log", lambda *_args, **_kwargs: None)
    stats = {}

    added = hb._fill_missing_from_exchange_perp_1m(
        exchange_id="bybit",
        symbol="BTC/USDT:USDT",
        coin="BTC",
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 2),
        sleep_s=0.0,
        stats_out=stats,
    )

    assert added == 0
    assert stats == {"days_requested": 1, "errors": 1}


def test_best_sync_start_bootstrap_prefers_oldest_api_day() -> None:
    d_start = date(2026, 2, 17)
    d_end = date(2026, 2, 21)
    api_days = [date(2026, 2, 18), date(2026, 2, 19), date(2026, 2, 20), date(2026, 2, 21)]

    sync_start, mode = _determine_best_sync_start(
        d_start=d_start,
        d_end=d_end,
        has_best_data=False,
        full_gap_days=0,
        api_days=api_days,
    )

    assert mode == "bootstrap"
    assert sync_start == date(2026, 2, 18)


def test_best_sync_start_catchup_uses_full_window() -> None:
    d_start = date(2026, 2, 17)
    d_end = date(2026, 2, 21)

    sync_start, mode = _determine_best_sync_start(
        d_start=d_start,
        d_end=d_end,
        has_best_data=True,
        full_gap_days=2,
        api_days=[date(2026, 2, 17), date(2026, 2, 21)],
    )

    assert mode == "catchup"
    assert sync_start == date(2026, 2, 17)


def test_best_sync_start_incremental_uses_last_two_days() -> None:
    d_start = date(2026, 2, 17)
    d_end = date(2026, 2, 21)

    sync_start, mode = _determine_best_sync_start(
        d_start=d_start,
        d_end=d_end,
        has_best_data=True,
        full_gap_days=0,
        api_days=[date(2026, 2, 17), date(2026, 2, 21)],
    )

    assert mode == "incremental"
    assert sync_start == date(2026, 2, 20)


def test_best_sync_start_bootstrap_without_api_days_falls_back_to_d_start() -> None:
    d_start = date(2026, 2, 17)
    d_end = date(2026, 2, 21)

    sync_start, mode = _determine_best_sync_start(
        d_start=d_start,
        d_end=d_end,
        has_best_data=False,
        full_gap_days=0,
        api_days=[],
    )

    assert mode == "bootstrap"
    assert sync_start == d_start


def test_best_sync_start_bootstrap_ignores_recent_lookback_clip_when_api_has_older_days() -> None:
    d_start = date(2026, 2, 19)
    d_end = date(2026, 2, 21)

    sync_start, mode = _determine_best_sync_start(
        d_start=d_start,
        d_end=d_end,
        has_best_data=False,
        full_gap_days=0,
        api_days=[date(2026, 2, 17), date(2026, 2, 18), date(2026, 2, 19)],
    )

    assert mode == "bootstrap"
    assert sync_start == date(2026, 2, 17)


def test_is_stock_perp_coin_detects_xyz_prefixes() -> None:
    assert _is_stock_perp_coin("xyz:TSLA") is True
    assert _is_stock_perp_coin("XYZ:TSLA/USDC:USDC") is True
    assert _is_stock_perp_coin("XYZ-TSLA/USDC:USDC") is True
    assert _is_stock_perp_coin("BTC") is False


def test_tradfi_ticker_from_hyperliquid_coin_extracts_base_ticker() -> None:
    assert _tradfi_ticker_from_hyperliquid_coin("xyz:TSLA") == "TSLA"
    assert _tradfi_ticker_from_hyperliquid_coin("XYZ:NVDA/USDC:USDC") == "NVDA"
    assert _tradfi_ticker_from_hyperliquid_coin("XYZ-AAPL/USDC:USDC") == "AAPL"


def test_improve_stock_perp_uses_tradfi_path_not_crypto(monkeypatch) -> None:
    d = date(2026, 2, 20)

    monkeypatch.setattr(hb, "TRADFI_IMPROVE_DEFAULT_LOOKBACK_DAYS", 0)
    monkeypatch.setattr(hb, "normalize_market_data_coin_dir", lambda *_args, **_kwargs: "XYZ:AAPL_USDC:USDC")
    monkeypatch.setattr(hb, "_determine_stock_perp_improve_start", lambda **_kwargs: d)
    monkeypatch.setattr(hb, "_list_api_days", lambda **_kwargs: [d])
    monkeypatch.setattr(hb, "_list_best_days", lambda **_kwargs: [])
    monkeypatch.setattr(hb, "get_local_l2book_day_range", lambda **_kwargs: None)
    monkeypatch.setattr(hb, "_is_stock_perp_coin", lambda _coin: True)

    monkeypatch.setattr(hb, "_read_day_npz", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(hb, "get_source_codes_for_day", lambda **_kwargs: None)

    def _never(*_args, **_kwargs):
        raise AssertionError("crypto path should not be used for stock-perp")

    monkeypatch.setattr(hb, "_l2book_minutes_for_day", _never)
    monkeypatch.setattr(hb, "_fill_missing_from_binance_perp_1m", _never)
    monkeypatch.setattr(hb, "_fill_missing_from_bybit_perp_1m", _never)

    called = {"ok": 0}

    def _tradfi_fill(**_kwargs):
        called["ok"] += 1
        return 12

    monkeypatch.setattr(hb, "_fill_missing_from_tradfi_1m", _tradfi_fill)

    res = hb.improve_best_hyperliquid_1m_archive_for_coin(
        coin="xyz:AAPL",
        end_date=d,
        dry_run=False,
    )

    assert called["ok"] == 1
    assert res.tiingo_minutes_filled == 12
    assert res.binance_minutes_filled == 0
    assert res.bybit_minutes_filled == 0


def test_stock_perp_start_with_tiingo_uses_iex_floor_despite_existing_marker(monkeypatch) -> None:
    d_end = date(2026, 2, 21)

    monkeypatch.setattr(hb, "get_oldest_day_with_source_code", lambda **_kwargs: "20200115")
    monkeypatch.setattr(hb, "_load_tradfi_profiles_from_ini", lambda: {"tiingo": {"api_key": "t", "enabled": "1"}})

    start = _determine_stock_perp_improve_start(
        coin_u="xyz:AAPL",
        coin_dir="XYZ:AAPL_USDC:USDC",
        d_end=d_end,
        earliest_candidates=[date(2026, 2, 17)],
        timeout_s=30.0,
    )

    assert start == date(2016, 12, 12)


def test_stock_perp_start_without_credentials_uses_earliest_candidates(monkeypatch) -> None:
    d_end = date(2026, 2, 21)

    monkeypatch.setattr(hb, "get_oldest_day_with_source_code", lambda **_kwargs: None)
    monkeypatch.setattr(hb, "_load_tradfi_profiles_from_ini", lambda: {"tiingo": {"api_key": "", "enabled": "0"}})

    start = _determine_stock_perp_improve_start(
        coin_u="xyz:AAPL",
        coin_dir="XYZ:AAPL_USDC:USDC",
        d_end=d_end,
        earliest_candidates=[date(2024, 2, 17), date(2023, 7, 1)],
        timeout_s=30.0,
    )

    assert start == date(2023, 7, 1)


def test_stock_perp_start_with_tiingo_uses_iex_floor(monkeypatch) -> None:
    d_end = date(2026, 2, 21)

    monkeypatch.setattr(hb, "get_oldest_day_with_source_code", lambda **_kwargs: None)
    monkeypatch.setattr(hb, "_load_tradfi_profiles_from_ini", lambda: {"tiingo": {"api_key": "tk", "enabled": "1"}})

    start = _determine_stock_perp_improve_start(
        coin_u="xyz:AMD",
        coin_dir="XYZ:AMD_USDC:USDC",
        d_end=d_end,
        earliest_candidates=[date(2026, 2, 17)],
        timeout_s=30.0,
    )

    assert start == date(2016, 12, 12)


def test_stock_perp_start_refetch_ignores_existing_other_marker(monkeypatch) -> None:
    d_end = date(2026, 2, 21)

    monkeypatch.setattr(hb, "get_oldest_day_with_source_code", lambda **_kwargs: "20200115")
    monkeypatch.setattr(hb, "_load_tradfi_profiles_from_ini", lambda: {"tiingo": {"api_key": "tk", "enabled": "1"}})

    start = _determine_stock_perp_improve_start(
        coin_u="xyz:AAPL",
        coin_dir="XYZ:AAPL_USDC:USDC",
        d_end=d_end,
        earliest_candidates=[date(2026, 2, 17)],
        refetch=True,
        timeout_s=30.0,
    )

    assert start == date(2016, 12, 12)


def test_tradfi_fill_uses_tiingo_iex_for_mapped_symbol(monkeypatch, tmp_path) -> None:
    day = date(2026, 2, 20)

    monkeypatch.setattr(hb, "normalize_hyperliquid_coin", lambda c: c)
    monkeypatch.setattr(hb, "normalize_market_data_coin_dir", lambda *_args, **_kwargs: "XYZ:AAPL_USDC:USDC")
    monkeypatch.setattr(hb, "_load_tradfi_profiles_from_ini", lambda: {"tiingo": {"api_key": "t", "enabled": "1"}})
    monkeypatch.setattr(hb, "_tradfi_ticker_from_hyperliquid_coin", lambda _c: "AAPL")
    monkeypatch.setattr(hb, "resolve_tradfi_symbol", lambda _xyz: ("AAPL", None, False, None))

    start_ms = int(datetime(day.year, day.month, day.day, 14, 30, tzinfo=timezone.utc).timestamp() * 1000)
    monkeypatch.setattr(
        hb,
        "_tiingo_fetch_1m_iex_day_from_month_cache",
        lambda **_kwargs: ([{"t": start_ms, "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 1.0}], True, True),
    )

    monkeypatch.setattr(hb, "get_source_codes_for_day", lambda **_kwargs: None)
    monkeypatch.setattr(hb, "update_source_index_for_day", lambda **_kwargs: None)
    monkeypatch.setattr(hb, "_read_day_npz", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(hb, "_write_day_npz", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(hb, "_best_day_path", lambda **_kwargs: tmp_path / "2026-02-20.npz")

    stats = {}
    added = hb._fill_missing_from_tradfi_1m(
        coin="xyz:AAPL",
        start_date=day,
        end_date=day,
        timeout_s=5.0,
        sleep_s=0.0,
        stats_out=stats,
    )

    assert added == 1
    assert int(stats.get("tiingo_minutes_filled") or 0) == 1
    assert int(stats.get("tiingo_month_requests_used") or 0) == 1


def test_tradfi_fill_tiingo_month_cache_reuses_single_request_across_days(monkeypatch, tmp_path) -> None:
    d1 = date(2026, 2, 19)
    d2 = date(2026, 2, 20)

    monkeypatch.setattr(hb, "normalize_hyperliquid_coin", lambda c: c)
    monkeypatch.setattr(hb, "normalize_market_data_coin_dir", lambda *_args, **_kwargs: "XYZ:AAPL_USDC:USDC")
    monkeypatch.setattr(hb, "_load_tradfi_profiles_from_ini", lambda: {"tiingo": {"api_key": "t", "enabled": "1"}})
    monkeypatch.setattr(hb, "_tradfi_ticker_from_hyperliquid_coin", lambda _c: "AAPL")
    monkeypatch.setattr(hb, "resolve_tradfi_symbol", lambda _xyz: ("AAPL", None, False, None))

    s1_start = int(datetime(d1.year, d1.month, d1.day, 14, 30, tzinfo=timezone.utc).timestamp() * 1000)
    s2_start = int(datetime(d2.year, d2.month, d2.day, 14, 30, tzinfo=timezone.utc).timestamp() * 1000)

    called_tiingo = {"n": 0}

    def _tiingo_called(**_kwargs):
        called_tiingo["n"] += 1
        return [
            {"t": s1_start, "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 1.0},
            {"t": s2_start, "o": 2.0, "h": 2.0, "l": 2.0, "c": 2.0, "v": 2.0},
        ]

    hb._TIINGO_MONTH_BAR_CACHE.clear()
    monkeypatch.setattr(hb, "_tiingo_fetch_1m_iex", _tiingo_called)

    monkeypatch.setattr(hb, "get_source_codes_for_day", lambda **_kwargs: None)
    monkeypatch.setattr(hb, "update_source_index_for_day", lambda **_kwargs: None)
    monkeypatch.setattr(hb, "_read_day_npz", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(hb, "_write_day_npz", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(hb, "_best_day_path", lambda **_kwargs: tmp_path / "test.npz")

    stats1 = {}
    stats2 = {}
    hb._fill_missing_from_tradfi_1m(
        coin="xyz:AAPL",
        start_date=d1,
        end_date=d1,
        timeout_s=5.0,
        sleep_s=0.0,
        stats_out=stats1,
    )
    hb._fill_missing_from_tradfi_1m(
        coin="xyz:AAPL",
        start_date=d2,
        end_date=d2,
        timeout_s=5.0,
        sleep_s=0.0,
        stats_out=stats2,
    )

    assert called_tiingo["n"] == 1
    assert int(stats1.get("tiingo_minutes_filled") or 0) == 1
    assert int(stats2.get("tiingo_minutes_filled") or 0) == 1
    assert int(stats1.get("tiingo_month_requests_used") or 0) == 1
    assert int(stats2.get("tiingo_month_requests_used") or 0) == 0


def test_tradfi_fill_skips_tiingo_when_session_already_complete(monkeypatch, tmp_path) -> None:
    day = _recent_weekday()
    day_s = day.strftime("%Y%m%d")

    monkeypatch.setattr(hb, "normalize_hyperliquid_coin", lambda c: c)
    monkeypatch.setattr(hb, "normalize_market_data_coin_dir", lambda *_args, **_kwargs: "XYZ:AAPL_USDC:USDC")
    monkeypatch.setattr(hb, "_load_tradfi_profiles_from_ini", lambda: {"tiingo": {"api_key": "t", "enabled": "1"}})
    monkeypatch.setattr(hb, "_tradfi_ticker_from_hyperliquid_coin", lambda _c: "AAPL")
    monkeypatch.setattr(hb, "resolve_tradfi_symbol", lambda _xyz: ("AAPL", None, False, None))

    session = hb._full_day_session_utc(day)
    assert session is not None
    session_start_ms, session_end_ms = session
    day_start_ms = hb._day_start_ms(day)
    start_idx = hb._minute_index(int(session_start_ms), int(day_start_ms))
    end_idx = hb._minute_index(int(session_end_ms), int(day_start_ms))

    existing = {
        int(idx): {
            "t": int(day_start_ms + idx * 60_000),
            "o": 1.0,
            "h": 1.0,
            "l": 1.0,
            "c": 1.0,
            "v": 0.0,
        }
        for idx in range(int(start_idx), int(end_idx) + 1)
    }

    called_tiingo = {"n": 0}

    def _tiingo_called(**_kwargs):
        called_tiingo["n"] += 1
        return ([], False, False)

    path = tmp_path / f"{day_s}.npz"
    path.write_text("stub", encoding="utf-8")
    monkeypatch.setattr(hb, "_best_day_path", lambda **_kwargs: path)
    monkeypatch.setattr(hb, "_read_day_npz", lambda *_args, **_kwargs: existing)
    monkeypatch.setattr(hb, "_write_day_npz", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(hb, "get_source_codes_for_day", lambda **_kwargs: None)
    monkeypatch.setattr(hb, "update_source_index_for_day", lambda **_kwargs: None)
    monkeypatch.setattr(hb, "_tiingo_fetch_1m_iex_day_from_month_cache", _tiingo_called)

    stats = {}
    added = hb._fill_missing_from_tradfi_1m(
        coin="xyz:AAPL",
        start_date=day,
        end_date=day,
        timeout_s=5.0,
        sleep_s=0.0,
        stats_out=stats,
    )

    assert called_tiingo["n"] == 0
    assert added == 0
    assert int(stats.get("tiingo_minutes_filled") or 0) == 0
    assert int(stats.get("tiingo_month_requests_used") or 0) == 0


def test_tradfi_fill_returns_zero_when_resolver_has_no_mapping(monkeypatch) -> None:
    monkeypatch.setattr(hb, "normalize_hyperliquid_coin", lambda c: c)
    monkeypatch.setattr(hb, "normalize_market_data_coin_dir", lambda *_args, **_kwargs: "XYZ:UNKNOWN_USDC:USDC")
    monkeypatch.setattr(hb, "_tradfi_ticker_from_hyperliquid_coin", lambda _c: "UNKNOWN")
    monkeypatch.setattr(hb, "resolve_tradfi_symbol", lambda _xyz: (None, None, False, None))

    added = hb._fill_missing_from_tradfi_1m(
        coin="xyz:UNKNOWN",
        start_date=date(2026, 2, 20),
        end_date=date(2026, 2, 20),
        timeout_s=5.0,
        sleep_s=0.0,
    )

    assert added == 0


def test_tradfi_fill_fx_path_sets_fx_stats(monkeypatch, tmp_path) -> None:
    day = date(2026, 2, 20)

    monkeypatch.setattr(hb, "normalize_hyperliquid_coin", lambda c: c)
    monkeypatch.setattr(hb, "normalize_market_data_coin_dir", lambda *_args, **_kwargs: "XYZ:EUR_USDC:USDC")
    monkeypatch.setattr(hb, "_load_tradfi_profiles_from_ini", lambda: {"tiingo": {"api_key": "t", "enabled": "1"}})
    monkeypatch.setattr(hb, "_tradfi_ticker_from_hyperliquid_coin", lambda _c: "EUR")
    monkeypatch.setattr(hb, "resolve_tradfi_symbol", lambda _xyz: (None, "EURUSD", True, None))

    start_ms = int(datetime(day.year, day.month, day.day, 14, 30, tzinfo=timezone.utc).timestamp() * 1000)
    monkeypatch.setattr(
        hb,
        "_tiingo_fetch_1m_fx_day_from_month_cache",
        lambda **_kwargs: ([{"t": start_ms, "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 1.0}], True, True),
    )

    monkeypatch.setattr(hb, "get_source_codes_for_day", lambda **_kwargs: None)
    monkeypatch.setattr(hb, "update_source_index_for_day", lambda **_kwargs: None)
    monkeypatch.setattr(hb, "_read_day_npz", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(hb, "_write_day_npz", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(hb, "_best_day_path", lambda **_kwargs: tmp_path / "fx.npz")

    stats = {}
    added = hb._fill_missing_from_tradfi_1m(
        coin="xyz:EUR",
        start_date=day,
        end_date=day,
        timeout_s=5.0,
        sleep_s=0.0,
        stats_out=stats,
    )

    assert added == 1
    assert int(stats.get("tiingo_fx_chunk_fetched") or 0) == 1
    assert int(stats.get("tiingo_fx_chunk_has_data") or 0) == 1
