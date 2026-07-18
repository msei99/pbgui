"""Tests for exchange 1m market-data downloader helpers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
import inspect
import json
import threading
import zipfile

import numpy as np
import pytest

import bitget_best_1m as bitget
import okx_best_1m as okx


def _ts_ms(year: int, month: int, day: int, hour: int, minute: int) -> int:
    """Return a UTC timestamp in milliseconds."""

    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


def _archive_zip_bytes(csv_text: str) -> bytes:
    """Build an in-memory OKX archive ZIP payload."""

    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("BTC-USDT-SWAP.csv", csv_text)
    return buffer.getvalue()


class _FakeResponse:
    """Minimal requests.Response test double for retry tests."""

    def __init__(self, status_code: int, payload: dict, text: str = "") -> None:
        """Store response state used by okx._okx_get_json."""

        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.content = b""

    def raise_for_status(self) -> None:
        """Raise for non-2xx responses like requests.Response."""

        if int(self.status_code) >= 400:
            raise okx.requests.HTTPError(f"HTTP {self.status_code}")

    def json(self) -> dict:
        """Return the configured JSON payload."""

        return self._payload


def test_parse_archive_zip_buckets_rows_by_utc_day_and_vol_ccy() -> None:
    """Archive rows are rebucketed by UTC date and use vol_ccy as bv."""

    csv_text = "\n".join([
        "instrument_name,open,high,low,close,vol,vol_ccy,vol_quote,open_time,confirm",
        f"BTC-USDT-SWAP,1,2,0.5,1.5,999,0.25,12,{_ts_ms(2024, 1, 1, 23, 59)},1",
        f"BTC-USDT-SWAP,3,4,2.5,3.5,888,0.5,14,{_ts_ms(2024, 1, 2, 0, 0)},1",
        f"ETH-USDT-SWAP,5,6,4.5,5.5,777,7.0,15,{_ts_ms(2024, 1, 2, 0, 1)},1",
    ])

    buckets = okx._parse_archive_zip(_archive_zip_bytes(csv_text), "BTC-USDT-SWAP")

    assert sorted(buckets.keys()) == ["2024-01-01", "2024-01-02"]
    assert buckets["2024-01-01"][1439]["v"] == 0.25
    assert buckets["2024-01-01"][1439]["raw_vol"] == 999.0
    assert buckets["2024-01-02"][0]["v"] == 0.5
    assert 1 not in buckets["2024-01-02"]


def test_parse_rest_row_uses_vol_ccy_not_raw_contract_volume() -> None:
    """REST candle parsing maps OKX volCcy to PBGui bv."""

    row = [str(_ts_ms(2024, 1, 1, 0, 0)), "1", "2", "0.5", "1.5", "999", "0.123", "20", "1"]

    candle = okx._parse_rest_row(row)

    assert candle is not None
    assert candle["v"] == 0.123
    assert candle["raw_vol"] == 999.0


def test_volume_enrichment_derives_contract_volume_before_rest(monkeypatch) -> None:
    """Missing archive vol_ccy is filled from OKX contract metadata before REST."""

    candles = {0: {"t": _ts_ms(2024, 1, 1, 0, 0), "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": None, "raw_vol": 10.0}}

    monkeypatch.setattr(okx, "_rest_fetch_range", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("REST should not be used")))
    monkeypatch.setattr(okx, "_get_contract_meta", lambda *_args, **_kwargs: {"ct_val": 0.01, "ct_val_ccy": "BTC", "base": "BTC", "quote": "USDT"})

    notes: list[str] = []
    enriched, pages = okx._enrich_missing_archive_volumes("BTC", "2024-01-01", candles, timeout_s=1.0, notes=notes)

    assert enriched == 1
    assert pages == 0
    assert candles[0]["v"] == 0.1
    assert notes == ["volume_contract_derived=2024-01-01:1"]


def test_volume_enrichment_falls_back_to_rest_when_contract_unresolved(monkeypatch) -> None:
    """REST volCcy is used when contract metadata cannot resolve base volume."""

    candles = {0: {"t": _ts_ms(2024, 1, 1, 0, 0), "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": None, "raw_vol": 10.0}}

    def fake_rest_fetch_range(*_args, **_kwargs):
        """Return REST volume for the missing archive minute."""

        return {"2024-01-01": {0: {"v": 2.5}}}, 1, []

    monkeypatch.setattr(okx, "_rest_fetch_range", fake_rest_fetch_range)
    monkeypatch.setattr(okx, "_get_contract_meta", lambda *_args, **_kwargs: {"ct_val": 0.0, "ct_val_ccy": "BTC", "base": "BTC", "quote": "USDT"})

    notes: list[str] = []
    enriched, pages = okx._enrich_missing_archive_volumes("BTC", "2024-01-01", candles, timeout_s=1.0, notes=notes)

    assert enriched == 1
    assert pages == 1
    assert candles[0]["v"] == 2.5
    assert notes == []


def test_bulk_volume_enrichment_uses_contracts_without_rest(monkeypatch) -> None:
    """Bulk archive volume enrichment derives contract volumes without REST calls."""

    day_buckets = {
        "2024-01-01": {0: {"t": _ts_ms(2024, 1, 1, 0, 0), "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": None, "raw_vol": 10.0}},
        "2024-01-02": {0: {"t": _ts_ms(2024, 1, 2, 0, 0), "o": 2.0, "h": 2.0, "l": 2.0, "c": 2.0, "v": None, "raw_vol": 20.0}},
    }
    monkeypatch.setattr(okx, "_rest_fetch_range", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("REST should not be used")))
    monkeypatch.setattr(okx, "_get_contract_meta", lambda *_args, **_kwargs: {"ct_val": 0.01, "ct_val_ccy": "BTC", "base": "BTC", "quote": "USDT"})

    notes: list[str] = []
    enriched, pages = okx._enrich_missing_archive_volumes_bulk("BTC", day_buckets, timeout_s=1.0, notes=notes)

    assert enriched == 2
    assert pages == 0
    assert day_buckets["2024-01-01"][0]["v"] == 0.1
    assert day_buckets["2024-01-02"][0]["v"] == 0.2
    assert notes == ["volume_contract_derived=2:2"]


def test_bulk_volume_enrichment_batches_rest_for_unresolved_contracts(monkeypatch) -> None:
    """Bulk archive volume enrichment keeps batched REST fallback when needed."""

    day_buckets = {
        "2024-01-01": {0: {"t": _ts_ms(2024, 1, 1, 0, 0), "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": None, "raw_vol": 10.0}},
        "2024-01-02": {0: {"t": _ts_ms(2024, 1, 2, 0, 0), "o": 2.0, "h": 2.0, "l": 2.0, "c": 2.0, "v": None, "raw_vol": 20.0}},
    }
    calls: list[tuple[int, int]] = []

    def fake_rest_fetch_range(_coin, since_ms, end_ms, **_kwargs):
        """Return REST volume for both consecutive days."""

        calls.append((since_ms, end_ms))
        return {
            "2024-01-01": {0: {"v": 1.5}},
            "2024-01-02": {0: {"v": 2.5}},
        }, 10, []

    monkeypatch.setattr(okx, "_rest_fetch_range", fake_rest_fetch_range)
    monkeypatch.setattr(okx, "_get_contract_meta", lambda *_args, **_kwargs: {"ct_val": 0.0, "ct_val_ccy": "BTC", "base": "BTC", "quote": "USDT"})

    notes: list[str] = []
    enriched, pages = okx._enrich_missing_archive_volumes_bulk("BTC", day_buckets, timeout_s=1.0, notes=notes)

    assert enriched == 2
    assert pages == 10
    assert len(calls) == 1
    assert day_buckets["2024-01-01"][0]["v"] == 1.5
    assert day_buckets["2024-01-02"][0]["v"] == 2.5
    assert notes == []


def test_repair_missing_minutes_writes_missing_rest_rows(monkeypatch, tmp_path) -> None:
    """Archive repair fetches a UTC day and writes only missing REST minutes."""

    monkeypatch.setattr(okx, "MIN_DAY_CANDLES", 3)
    day_path = tmp_path / "2024-01-01.npz"
    existing = {0: {"t": _ts_ms(2024, 1, 1, 0, 0), "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 1.0}}
    repaired = {
        0: existing[0],
        1: {"t": _ts_ms(2024, 1, 1, 0, 1), "o": 2.0, "h": 2.0, "l": 2.0, "c": 2.0, "v": 2.0},
        2: {"t": _ts_ms(2024, 1, 1, 0, 2), "o": 3.0, "h": 3.0, "l": 3.0, "c": 3.0, "v": 3.0},
    }
    reads = iter([existing, repaired])
    written: dict = {}

    def fake_rest_fetch_range(*_args, **_kwargs):
        """Return REST rows for the two missing minutes."""

        return {"2024-01-01": {1: repaired[1], 2: repaired[2]}}, 1, []

    def fake_write_candles_for_day(*args, **kwargs):
        """Record repair write parameters and report two written minutes."""

        written["args"] = args
        written["kwargs"] = kwargs
        return len(args[2])

    monkeypatch.setattr(okx, "_okx_day_path", lambda *_args, **_kwargs: day_path)
    monkeypatch.setattr(okx, "_read_day_npz", lambda *_args, **_kwargs: next(reads))
    monkeypatch.setattr(okx, "_rest_fetch_range", fake_rest_fetch_range)
    monkeypatch.setattr(okx, "_write_candles_for_day", fake_write_candles_for_day)

    written_count, fetched_count, remaining = okx._repair_missing_minutes_for_day("BTC", "2024-01-01", timeout_s=1.0)

    assert written_count == 2
    assert fetched_count == 2
    assert remaining == 0
    assert written["args"][:2] == ("BTC", "2024-01-01")
    assert sorted(written["args"][2].keys()) == [1, 2]
    assert written["kwargs"] == {"overwrite": False, "source_code": okx.SOURCE_CODE_API}


def test_okx_get_json_retries_429(monkeypatch) -> None:
    """OKX JSON requests retry transient 429 responses."""

    responses = iter([
        _FakeResponse(429, {"code": "0", "data": []}, text="rate limit"),
        _FakeResponse(200, {"code": "0", "data": [["ok"]]}),
    ])
    calls: list[dict] = []

    def fake_get(url, params=None, headers=None, timeout=None):
        """Return one fake response per request call."""

        calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        return next(responses)

    monkeypatch.setattr(okx.requests, "get", fake_get)
    monkeypatch.setattr(okx.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(okx.random, "random", lambda: 0.0)

    payload = okx._okx_get_json(okx.HISTORY_ENDPOINT, {"instId": "BTC-USDT-SWAP"}, retries=2)

    assert payload == {"code": "0", "data": [["ok"]]}
    assert len(calls) == 2
    assert calls[0]["headers"]["User-Agent"].startswith("PBGui/")


def test_okx_symbol_normalization_and_storage_directory(monkeypatch) -> None:
    """OKX symbol variants normalize without changing PB7-compatible storage names."""

    monkeypatch.setattr(okx, "_load_okx_usdt_map", lambda: {"BONK": "1000BONK-USDT-SWAP"})

    for value in ("BTC", "BTCUSDT", "BTC/USDT:USDT", "BTC_USDT:USDT", "BTC-USDT-SWAP"):
        assert okx._coin_to_okx_inst_id(value) == "BTC-USDT-SWAP"
        assert okx.get_storage_coin_dir(value) == "BTC_USDT:USDT"
    assert okx._coin_to_okx_inst_id("BONK") == "1000BONK-USDT-SWAP"
    assert okx.get_storage_coin_dir("BONK") == "1000BONK_USDT:USDT"
    assert okx.STORAGE_EXCHANGE == "okx"


@pytest.mark.parametrize("failure", ["network", "timeout", "server", "json", "api"])
def test_okx_json_transient_failures_retry(monkeypatch, failure: str) -> None:
    """Transport, timeout, 5xx, malformed JSON, and transient API failures retry."""

    calls = 0

    class InvalidJsonResponse(_FakeResponse):
        """Represent a successful response with malformed JSON."""

        def json(self) -> dict:
            """Raise the JSON parser failure under test."""

            raise ValueError("invalid json")

    def fake_get(*_args, **_kwargs):
        """Fail once according to the parameter, then return success."""

        nonlocal calls
        calls += 1
        if calls > 1:
            return _FakeResponse(200, {"code": "0", "data": []})
        if failure == "network":
            raise okx.requests.ConnectionError("offline")
        if failure == "timeout":
            raise okx.requests.Timeout("timed out")
        if failure == "server":
            return _FakeResponse(503, {}, text="unavailable")
        if failure == "json":
            return InvalidJsonResponse(200, {})
        return _FakeResponse(200, {"code": "50013", "msg": "busy", "data": None})

    monkeypatch.setattr(okx.requests, "get", fake_get)
    monkeypatch.setattr(okx.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(okx.random, "random", lambda: 0.0)

    assert okx._okx_get_json(okx.HISTORY_ENDPOINT, {}, retries=2) == {"code": "0", "data": []}
    assert calls == 2


@pytest.mark.parametrize("failure", ["network", "timeout", "server"])
def test_okx_archive_download_transient_failures_retry(monkeypatch, failure: str) -> None:
    """Archive downloads retry transport, timeout, and server failures."""

    calls = 0

    def fake_get(*_args, **_kwargs):
        """Fail once according to the parameter, then return archive bytes."""

        nonlocal calls
        calls += 1
        if calls > 1:
            response = _FakeResponse(200, {})
            response.content = b"archive"
            return response
        if failure == "network":
            raise okx.requests.ConnectionError("offline")
        if failure == "timeout":
            raise okx.requests.Timeout("timed out")
        return _FakeResponse(502, {}, text="unavailable")

    monkeypatch.setattr(okx.requests, "get", fake_get)
    monkeypatch.setattr(okx.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(okx.random, "random", lambda: 0.0)

    assert okx._download_bytes("https://public.invalid/archive.zip", retries=2) == b"archive"
    assert calls == 2


def test_okx_429_retry_exhaustion_is_sanitized_runtime_error(monkeypatch) -> None:
    """Repeated throttling exhausts retries without exposing request parameters."""

    calls = 0

    def fake_get(*_args, **_kwargs):
        """Return one more throttled response."""

        nonlocal calls
        calls += 1
        return _FakeResponse(429, {}, text="secret-value")

    monkeypatch.setattr(okx.requests, "get", fake_get)
    monkeypatch.setattr(okx.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(okx.random, "random", lambda: 0.0)

    with pytest.raises(RuntimeError, match="HTTP 429") as exc_info:
        okx._okx_get_json(okx.HISTORY_ENDPOINT, {"token": "secret-value"}, retries=2)

    assert calls == 2
    assert "secret-value" not in str(exc_info.value)


@pytest.mark.parametrize("failure", ["http", "api", "type", "key"])
def test_okx_json_nonretryable_failures_surface_immediately(monkeypatch, failure: str) -> None:
    """HTTP 4xx, ordinary API codes, and local programming errors do not retry."""

    calls = 0

    class KeyErrorResponse(_FakeResponse):
        """Raise a local key error while decoding the payload."""

        def json(self) -> dict:
            """Raise the local programming error under test."""

            raise KeyError("bad decoder")

    def fake_get(*_args, **_kwargs):
        """Return or raise the configured non-transient failure."""

        nonlocal calls
        calls += 1
        if failure == "http":
            return _FakeResponse(404, {}, text="not found")
        if failure == "api":
            return _FakeResponse(200, {"code": "51000", "msg": "bad parameter"})
        if failure == "type":
            raise TypeError("bad local call")
        return KeyErrorResponse(200, {})

    monkeypatch.setattr(okx.requests, "get", fake_get)

    expected = RuntimeError if failure in {"http", "api"} else (TypeError if failure == "type" else KeyError)
    with pytest.raises(expected):
        okx._okx_get_json(okx.HISTORY_ENDPOINT, {}, retries=8)

    assert calls == 1


@pytest.mark.parametrize("failure", ["http", "local"])
def test_okx_archive_nonretryable_failures_surface_immediately(monkeypatch, failure: str) -> None:
    """Archive HTTP 4xx and local adapter errors fail without retries."""

    calls = 0

    def fake_get(*_args, **_kwargs):
        """Return or raise one non-transient archive failure."""

        nonlocal calls
        calls += 1
        if failure == "http":
            return _FakeResponse(403, {}, text="forbidden")
        raise TypeError("bad local call")

    monkeypatch.setattr(okx.requests, "get", fake_get)

    expected = RuntimeError if failure == "http" else TypeError
    with pytest.raises(expected):
        okx._download_bytes("https://public.invalid/archive.zip", retries=8)

    assert calls == 1


def test_okx_retry_stops_before_starting_another_request(monkeypatch) -> None:
    """A stop raised during a transient response prevents another retry request."""

    stopped = threading.Event()
    calls = 0

    def fake_get(*_args, **_kwargs):
        """Set stop while returning a retryable server response."""

        nonlocal calls
        calls += 1
        stopped.set()
        return _FakeResponse(503, {}, text="unavailable")

    monkeypatch.setattr(okx.requests, "get", fake_get)

    with pytest.raises(okx._OkxStopped):
        okx._okx_get_json(okx.HISTORY_ENDPOINT, {}, retries=8, stop_check=stopped.is_set)

    assert calls == 1


def test_okx_inception_failure_does_not_return_empty_success(monkeypatch) -> None:
    """An exhausted inception request fails instead of becoming no-data success."""

    logs: list[tuple[str, str | None]] = []
    monkeypatch.setattr(okx, "_coin_to_okx_inst_id", lambda _coin: "BTC-USDT-SWAP")
    monkeypatch.setattr(
        okx,
        "_has_rest_data_before",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("request exhausted")),
    )
    monkeypatch.setattr(
        okx,
        "append_exchange_download_log",
        lambda _exchange, line, level=None: logs.append((line, level)),
    )

    with pytest.raises(RuntimeError, match="request exhausted"):
        okx.improve_best_okx_1m_for_coin(coin="BTC", end_date="2024-01-01")

    assert logs
    assert "inception_error" in logs[0][0]
    assert logs[0][1] == "WARNING"


def test_write_candles_for_day_persists_npz_and_source_index(monkeypatch, tmp_path) -> None:
    """Daily writes use PBGui NPZ schema and update OKX source indexes."""

    root = tmp_path / "ohlcv" / "okx"
    updates: list[dict] = []
    candle = {"t": _ts_ms(2024, 1, 1, 0, 0), "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 0.25}

    monkeypatch.setattr(okx, "get_exchange_raw_root_dir", lambda _exchange: root)
    monkeypatch.setattr(okx, "update_source_index_for_day", lambda **kwargs: updates.append(kwargs))

    added = okx._write_candles_for_day("BTC", "2024-01-01", {0: candle}, overwrite=False, source_code=okx.SOURCE_CODE_API)

    path = root / "1m" / "BTC_USDT:USDT" / "2024-01-01.npz"
    assert added == 1
    assert path.exists()
    with np.load(path) as data:
        arr = data["candles"]
    assert arr.dtype.names == ("ts", "o", "h", "l", "c", "bv")
    assert int(arr[0]["ts"]) == candle["t"]
    assert float(arr[0]["bv"]) == np.float32(0.25)
    assert updates == [{
        "exchange": "okx",
        "coin": "BTC_USDT:USDT",
        "day": "2024-01-01",
        "minute_indices": [0],
        "code": okx.SOURCE_CODE_API,
    }]


def test_bitget_btc_symbol_inputs_resolve_to_native_and_storage_dir() -> None:
    """BTC variants resolve to BTCUSDT and BTC_USDT:USDT."""

    assert bitget._coin_to_bitget_symbol("BTC") == "BTCUSDT"
    assert bitget._coin_to_bitget_symbol("BTCUSDT") == "BTCUSDT"
    assert bitget._coin_to_bitget_symbol("BTC/USDT:USDT") == "BTCUSDT"
    assert bitget.get_storage_coin_dir("BTC_USDT:USDT") == "BTC_USDT:USDT"


def test_bitget_mapping_preserves_power_of_ten_base_prefix() -> None:
    """Mapped power-of-ten markets keep their native Bitget base prefix."""

    assert bitget._coin_to_bitget_symbol("BONK") == "1000BONKUSDT"
    assert bitget.get_storage_coin_dir("BONK") == "1000BONK_USDT:USDT"


def test_bitget_rest_row_maps_base_volume_to_bv() -> None:
    """Bitget row index 5 is stored as base-volume value."""

    row = ["1711929600000", "1", "2", "0.5", "1.5", "27.59", "41.385"]
    parsed = bitget._parse_rest_row(row)

    assert parsed == {"t": 1711929600000, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 27.59}


def test_bitget_bucket_rows_dedupes_overlapping_minutes() -> None:
    """Duplicate rows for the same day/minute collapse to one candle."""

    rows = [
        ["1711929600000", "1", "2", "0.5", "1.5", "10", "15"],
        ["1711929600000", "1", "2", "0.5", "1.6", "11", "17.6"],
    ]

    buckets = bitget._bucket_rows(rows, since_ms=1711929600000, end_ms=1711929660000)

    assert list(buckets.keys()) == ["2024-04-01"]
    assert len(buckets["2024-04-01"]) == 1
    assert buckets["2024-04-01"][0]["c"] == 1.6
    assert buckets["2024-04-01"][0]["v"] == 11.0


def test_bitget_bucket_rows_enforces_global_range_boundaries() -> None:
    """Global range filtering preserves start_date_override semantics."""

    rows = [
        ["1711929540000", "1", "1", "1", "1", "1", "1"],
        ["1711929600000", "2", "2", "2", "2", "2", "4"],
        ["1711929660000", "3", "3", "3", "3", "3", "9"],
    ]

    buckets = bitget._bucket_rows(rows, since_ms=1711929600000, end_ms=1711929660000)

    assert len(buckets["2024-04-01"]) == 1
    assert buckets["2024-04-01"][0]["o"] == 2.0


def test_bitget_write_day_npz_uses_candles_key_and_dtype(tmp_path) -> None:
    """Written NPZ files match PBGui's structured candle schema."""

    path = tmp_path / "2024-04-01.npz"
    bitget._write_day_npz(
        path,
        {
            0: {"t": 1711929600000, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 27.59},
        },
    )

    with np.load(path) as data:
        assert "candles" in data
        arr = data["candles"]
    assert arr.dtype == bitget._NPZ_DTYPE
    assert arr[0]["bv"] == np.float32(27.59)


def test_bitget_build_end_time_cursors_uses_200_minute_steps() -> None:
    """A 401-minute range needs three descending endTime cursors."""

    start = 1_000_000
    end = start + (401 * bitget.MS_PER_MINUTE)

    assert bitget._build_end_time_cursors(start, end) == [
        end,
        end - (200 * bitget.MS_PER_MINUTE),
        end - (400 * bitget.MS_PER_MINUTE),
    ]


def test_bitget_defaults_stay_below_official_public_rate_limit() -> None:
    """Bitget REST defaults leave headroom below the 20 req/s IP endpoint limit."""

    assert bitget.REST_RATE_PER_SECOND == 18.0
    assert bitget.REST_WORKERS == 16
    assert bitget.MAX_RETRIES >= 8


def test_bitget_429_retries_with_rate_limit_penalty(monkeypatch) -> None:
    """HTTP 429 responses back off before retrying the request."""

    class FakeResponse:
        """Minimal response object for Bitget retry tests."""

        def __init__(self, status_code: int, payload: dict) -> None:
            self.status_code = status_code
            self._payload = payload
            self.text = '{"code":"429","msg":"Too Many Requests"}' if status_code == 429 else '{"code":"00000"}'

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._payload

    class FakeSession:
        """Return one 429 response followed by a successful payload."""

        def __init__(self) -> None:
            self.calls = 0

        def get(self, *_args, **_kwargs) -> FakeResponse:
            self.calls += 1
            if self.calls == 1:
                return FakeResponse(429, {"code": "429", "msg": "Too Many Requests", "data": None})
            return FakeResponse(200, {"code": "00000", "data": []})

    fake_session = FakeSession()
    sleeps: list[float] = []
    monkeypatch.setattr(bitget, "_get_session", lambda: fake_session)
    monkeypatch.setattr(bitget.random, "random", lambda: 0.0)
    monkeypatch.setattr(bitget.time, "sleep", lambda seconds: sleeps.append(float(seconds)))

    payload = bitget._bitget_get_json(
        bitget.HISTORY_ENDPOINT,
        {"symbol": "BTCUSDT", "productType": bitget.PRODUCT_TYPE, "granularity": bitget.GRANULARITY},
        limiter=bitget.RateLimiter(1000.0),
    )

    assert payload == {"code": "00000", "data": []}
    assert fake_session.calls == 2
    assert any(seconds >= bitget.RATE_LIMIT_PENALTY_S for seconds in sleeps)


def test_bitget_40034_raises_unavailable_symbol(monkeypatch) -> None:
    """Bitget symbol-not-exists responses stay non-retryable and typed."""

    class FakeSession:
        """Return Bitget's unavailable-symbol response."""

        def get(self, *_args, **_kwargs) -> _FakeResponse:
            return _FakeResponse(400, {"code": "40034", "msg": "Symbol not exists"}, text='{"code":"40034","msg":"Symbol not exists"}')

    monkeypatch.setattr(bitget, "_get_session", lambda: FakeSession())

    try:
        bitget._bitget_get_json(
            bitget.HISTORY_ENDPOINT,
            {"symbol": "HYUNUSDT", "productType": bitget.PRODUCT_TYPE, "granularity": bitget.GRANULARITY},
        )
    except bitget.BitgetUnavailableSymbolError as exc:
        assert "40034" in str(exc)
        assert "Symbol not exists" in str(exc)
    else:
        raise AssertionError("expected BitgetUnavailableSymbolError")


def test_okx_latest_success_logs_info_even_with_errors_field(monkeypatch) -> None:
    """OKX latest success logs must not be auto-classified as ERROR by errors=0."""

    logs: list[tuple[str, str, str | None]] = []
    monkeypatch.setattr(okx, "_rest_fetch_range", lambda *_args, **_kwargs: ({}, 0, []))
    monkeypatch.setattr(okx, "append_exchange_download_log", lambda exchange, line, level=None: logs.append((exchange, line, level)))

    result = okx.update_latest_okx_1m_for_coin(coin="BTC", lookback_days=2)

    assert result["result"] == "ok"
    done_logs = [item for item in logs if "[okx_latest_1m] done" in item[1]]
    assert done_logs
    assert done_logs[-1][2] == "INFO"


def test_bitget_market_data_dispatch_tables_include_bitget() -> None:
    """Best-1m and status dispatch tables expose Bitget."""

    import task_worker
    from api import market_data

    assert market_data.BEST_1M_EXCHANGES["bitget"]["job_type"] == "bitget_best_1m"
    assert market_data.BEST_1M_EXCHANGES["bitget"]["queue_exchange"] == "bitget"
    assert market_data.COPY_DATA_EXCHANGES["bitget"]["storage"] == "bitget"
    assert task_worker.OHLCV_COPY_EXCHANGES["bitget"]["storage"] == "bitget"
    assert market_data._get_exchange_status_key("bitget") == "bitget_latest_1m"
    assert market_data._get_exchange_flag_prefix("bitget") == "bitget_latest_1m"


def test_bitget_heatmap_lag_uses_bitget_ini_section(monkeypatch) -> None:
    """Bitget heatmap lag reads bitget_data instead of pbdata defaults."""

    import pbgui_purefunc
    from api import heatmap

    def fake_load_ini(section, key):
        """Return a Bitget-specific latest-1m interval."""

        if section == "bitget_data" and key == "latest_1m_interval_seconds":
            return "7200"
        return "1800"

    monkeypatch.setattr(pbgui_purefunc, "load_ini", fake_load_ini)

    assert heatmap._get_missing_lag_minutes("bitget") == 120


def test_bitget_day_helpers_return_yyyymmdd(monkeypatch) -> None:
    """Newest/oldest helpers return compact YYYYMMDD strings."""

    monkeypatch.setattr(bitget, "_list_existing_days", lambda coin: [date(2024, 4, 1), date(2024, 4, 3)])

    assert bitget.get_oldest_day("BTC") == "20240401"
    assert bitget.get_newest_day("BTC") == "20240403"


def test_bitget_missing_minute_detection_and_day_repair(monkeypatch) -> None:
    """Bitget repair returns only candles from the requested UTC day."""

    day_s = "2024-04-01"
    day_start = _ts_ms(2024, 4, 1, 0, 0)
    candle = {"t": day_start + 60_000, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 3.0}
    calls: list[tuple[int, int]] = []

    def fake_rest_fetch_range(_coin, since_ms, end_ms, **_kwargs):
        """Return one repaired minute from the exact requested day."""

        calls.append((since_ms, end_ms))
        return {day_s: {1: candle}}, 1

    monkeypatch.setattr(bitget, "_rest_fetch_range", fake_rest_fetch_range)

    repaired, fetched = bitget._repair_missing_minutes_for_day("BTC", day_s)

    assert bitget._validate_day_minutes({0: candle, 1439: candle}) == list(range(1, 1439))
    assert repaired == {1: candle}
    assert fetched == 1
    assert calls == [(day_start, day_start + bitget.DAY_MS)]


def test_bitget_inception_and_current_day_skip_unrepaired_notes(monkeypatch) -> None:
    """The full listing day and current UTC day are exempt from repair warnings."""

    today = date(2026, 7, 18)
    inception_day = today - timedelta(days=1)
    repair_calls: list[str] = []

    class FixedDateTime(datetime):
        """Keep the UTC day stable across the boundary test."""

        @classmethod
        def now(cls, tz=None):
            """Return the fixed current instant."""

            return cls(2026, 7, 18, 12, 0, tzinfo=tz)

    monkeypatch.setattr(bitget, "datetime", FixedDateTime)
    monkeypatch.setattr(bitget, "_find_inception_ms", lambda *_args, **_kwargs: bitget._day_start_ms(inception_day))
    monkeypatch.setattr(bitget, "_read_day_npz", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(bitget, "_rest_fetch_range", lambda *_args, **_kwargs: ({}, 0))
    monkeypatch.setattr(
        bitget,
        "_repair_missing_minutes_for_day",
        lambda _coin, day_s, **_kwargs: (repair_calls.append(day_s) or {}, 0),
    )
    monkeypatch.setattr(bitget, "append_exchange_download_log", lambda *_args, **_kwargs: None)

    result = bitget.improve_best_bitget_1m_for_coin(coin="BTC", end_date=today)

    assert repair_calls == []
    assert not any(note.startswith("unrepaired_minutes=") for note in result.notes)


def test_bitget_historical_gap_is_reported_after_failed_repair(monkeypatch) -> None:
    """A historical complete-day gap remains visible when REST cannot repair it."""

    inception_day = date(2024, 4, 1)
    target_day = inception_day + timedelta(days=1)
    monkeypatch.setattr(bitget, "_find_inception_ms", lambda *_args, **_kwargs: bitget._day_start_ms(inception_day))
    monkeypatch.setattr(bitget, "_read_day_npz", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(bitget, "_rest_fetch_range", lambda *_args, **_kwargs: ({}, 0))
    monkeypatch.setattr(bitget, "_repair_missing_minutes_for_day", lambda *_args, **_kwargs: ({}, 0))
    monkeypatch.setattr(bitget, "append_exchange_download_log", lambda *_args, **_kwargs: None)

    result = bitget.improve_best_bitget_1m_for_coin(
        coin="BTC",
        start_date_override=target_day,
        end_date=target_day,
    )

    assert f"unrepaired_minutes={target_day:%Y-%m-%d}:1440" in result.notes


def test_bitget_successful_repair_merges_missing_minutes(monkeypatch) -> None:
    """A historical repair merges missing minutes and clears the warning."""

    inception_day = date(2024, 4, 1)
    target_day = inception_day + timedelta(days=1)
    target_s = target_day.strftime("%Y-%m-%d")
    start_ms = bitget._day_start_ms(target_day)
    stored: dict[int, dict] = {}
    writes: list[tuple[bool, list[int]]] = []

    def candle(index: int) -> dict:
        """Build one candle for the target minute index."""

        return {"t": start_ms + (index * bitget.MS_PER_MINUTE), "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 1.0}

    def fake_rest_fetch_range(*_args, **_kwargs):
        """Return the first minute during the initial range fetch."""

        return {target_s: {0: candle(0)}}, 1

    def fake_write(_coin, _day, candles, *, overwrite=False, **_kwargs):
        """Apply writes to the in-memory day used by the test."""

        if overwrite:
            stored.clear()
        before = len(stored)
        stored.update(candles)
        writes.append((bool(overwrite), sorted(candles)))
        return len(stored) - before

    monkeypatch.setattr(bitget, "MIN_DAY_CANDLES", 3)
    monkeypatch.setattr(bitget, "_find_inception_ms", lambda *_args, **_kwargs: bitget._day_start_ms(inception_day))
    monkeypatch.setattr(bitget, "_read_day_npz", lambda *_args, **_kwargs: dict(stored))
    monkeypatch.setattr(bitget, "_rest_fetch_range", fake_rest_fetch_range)
    monkeypatch.setattr(bitget, "_repair_missing_minutes_for_day", lambda *_args, **_kwargs: ({1: candle(1), 2: candle(2)}, 2))
    monkeypatch.setattr(bitget, "_write_candles_for_day", fake_write)
    monkeypatch.setattr(bitget, "append_exchange_download_log", lambda *_args, **_kwargs: None)

    result = bitget.improve_best_bitget_1m_for_coin(
        coin="BTC",
        start_date_override=target_day,
        end_date=target_day,
    )

    assert sorted(stored) == [0, 1, 2]
    assert writes == [(True, [0]), (False, [1, 2])]
    assert result.repair_minutes_fetched == 2
    assert result.minutes_written == 3
    assert not any(note.startswith("unrepaired_minutes=") for note in result.notes)


class _BitgetFakeLimiter:
    """Record Bitget limiter calls without wall-clock sleeps."""

    def __init__(self) -> None:
        """Initialize empty limiter counters."""

        self.waits = 0
        self.penalties: list[float] = []

    def wait(self) -> None:
        """Record one request slot."""

        self.waits += 1

    def penalize(self, seconds: float) -> None:
        """Record one shared rate-limit penalty."""

        self.penalties.append(float(seconds))


def test_bitget_json_429_retries_and_penalizes_all_workers(monkeypatch) -> None:
    """A JSON-body 429 retries and applies the shared limiter penalty."""

    responses = iter([
        _FakeResponse(200, {"code": "42901", "msg": "rate limited", "data": None}),
        _FakeResponse(200, {"code": "00000", "data": []}),
    ])
    calls = 0

    class FakeSession:
        """Return the configured Bitget JSON responses."""

        def get(self, *_args, **_kwargs):
            """Return the next response."""

            nonlocal calls
            calls += 1
            return next(responses)

    limiter = _BitgetFakeLimiter()
    monkeypatch.setattr(bitget, "_get_session", lambda: FakeSession())
    monkeypatch.setattr(bitget.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(bitget.random, "random", lambda: 0.0)

    payload = bitget._bitget_get_json(bitget.HISTORY_ENDPOINT, {}, retries=2, limiter=limiter)

    assert payload == {"code": "00000", "data": []}
    assert calls == 2
    assert limiter.waits == 2
    assert limiter.penalties == [bitget.RATE_LIMIT_PENALTY_S]


def test_bitget_429_retry_exhaustion_surfaces_failure(monkeypatch) -> None:
    """Repeated Bitget throttling fails after the configured retry count."""

    calls = 0

    class FakeSession:
        """Always return an HTTP 429 response."""

        def get(self, *_args, **_kwargs):
            """Return one more throttled response."""

            nonlocal calls
            calls += 1
            return _FakeResponse(429, {"code": "429", "msg": "rate limited"}, text="rate limited")

    monkeypatch.setattr(bitget, "_get_session", lambda: FakeSession())
    monkeypatch.setattr(bitget.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(bitget.random, "random", lambda: 0.0)

    with pytest.raises(RuntimeError, match="Bitget GET failed"):
        bitget._bitget_get_json(bitget.HISTORY_ENDPOINT, {}, retries=2, limiter=_BitgetFakeLimiter())

    assert calls == 2


@pytest.mark.parametrize("failure", ["network", "server", "json"])
def test_bitget_transient_failures_retry(monkeypatch, failure: str) -> None:
    """Network, server, and JSON failures retry before returning success."""

    calls = 0

    class InvalidJsonResponse(_FakeResponse):
        """Represent a successful HTTP response with malformed JSON."""

        def json(self) -> dict:
            """Raise the parser failure under test."""

            raise ValueError("invalid json")

    class FakeSession:
        """Fail once according to the parameter, then succeed."""

        def get(self, *_args, **_kwargs):
            """Return one transient failure followed by success."""

            nonlocal calls
            calls += 1
            if calls > 1:
                return _FakeResponse(200, {"code": "00000", "data": []})
            if failure == "network":
                raise bitget.requests.ConnectionError("offline")
            if failure == "server":
                return _FakeResponse(503, {}, text="unavailable")
            return InvalidJsonResponse(200, {})

    monkeypatch.setattr(bitget, "_get_session", lambda: FakeSession())
    monkeypatch.setattr(bitget.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(bitget.random, "random", lambda: 0.0)

    payload = bitget._bitget_get_json(bitget.HISTORY_ENDPOINT, {}, retries=2)

    assert payload == {"code": "00000", "data": []}
    assert calls == 2


@pytest.mark.parametrize("code", ["40053", "400171"])
def test_bitget_programming_errors_do_not_retry(monkeypatch, code: str) -> None:
    """Invalid request parameters surface immediately without retry loops."""

    calls = 0

    class FakeSession:
        """Return one non-retryable Bitget API error."""

        def get(self, *_args, **_kwargs):
            """Return the configured programming error."""

            nonlocal calls
            calls += 1
            return _FakeResponse(200, {"code": code, "msg": "invalid request", "data": None})

    monkeypatch.setattr(bitget, "_get_session", lambda: FakeSession())

    with pytest.raises(RuntimeError, match=code):
        bitget._bitget_get_json(bitget.HISTORY_ENDPOINT, {}, retries=8)

    assert calls == 1


def test_bitget_local_programming_error_does_not_retry(monkeypatch) -> None:
    """Unexpected local exceptions surface immediately instead of being retried."""

    calls = 0

    class FakeSession:
        """Raise one local programming error from the request adapter."""

        def get(self, *_args, **_kwargs):
            """Raise the non-transient error."""

            nonlocal calls
            calls += 1
            raise TypeError("bad local call")

    monkeypatch.setattr(bitget, "_get_session", lambda: FakeSession())

    with pytest.raises(TypeError, match="bad local call"):
        bitget._bitget_get_json(bitget.HISTORY_ENDPOINT, {}, retries=8)

    assert calls == 1


def test_bitget_session_is_reused_within_worker_thread(monkeypatch) -> None:
    """Each worker reuses its own session without sharing it across threads."""

    created: list[object] = []
    created_lock = threading.Lock()
    barrier = threading.Barrier(2)

    class FakeSession:
        """Record adapter mounts for a newly created worker session."""

        def __init__(self) -> None:
            """Initialize an empty mount map."""

            self.mounts: dict[str, object] = {}

        def mount(self, prefix: str, adapter: object) -> None:
            """Record one transport adapter."""

            self.mounts[prefix] = adapter

    def session_factory():
        """Create and record one fake requests session."""

        session = FakeSession()
        with created_lock:
            created.append(session)
        return session

    def worker() -> tuple[object, object]:
        """Return two session lookups from one worker thread."""

        first = bitget._get_session()
        barrier.wait(timeout=2)
        return first, bitget._get_session()

    monkeypatch.setattr(bitget, "_THREAD_LOCAL", threading.local())
    monkeypatch.setattr(bitget.requests, "Session", session_factory)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: worker(), range(2)))

    assert all(first is second for first, second in results)
    assert results[0][0] is not results[1][0]
    assert len(created) == 2
    assert all(set(session.mounts) == {"https://", "http://"} for session in created)


def test_bitget_coin_options_request_usdt_swaps_only(monkeypatch) -> None:
    """Bitget coin options pass the required USDT quote filter to CoinData."""

    import market_data

    captured: dict = {}

    class FakeCoinData:
        """Return one approved coin while recording filter arguments."""

        def filter_mapping(self, **kwargs):
            """Capture mapping filters and return one coin."""

            captured.update(kwargs)
            return ["BTC"], []

    monkeypatch.setattr(market_data, "CoinData", FakeCoinData)
    monkeypatch.setattr(market_data, "_filter_live_market_data_coin_options", lambda _exchange, coins: coins)

    assert market_data.get_market_data_coin_options("bitget") == ["BTC"]
    assert captured["quote_filter"] == ["USDT"]
    assert captured["active_only"] is True


def test_bitget_coin_options_fallback_excludes_non_usdt_or_inactive_markets(monkeypatch, tmp_path) -> None:
    """The mapping fallback accepts only active linear USDT swaps."""

    import market_data

    mapping_path = tmp_path / "data" / "coindata" / "bitget" / "mapping.json"
    mapping_path.parent.mkdir(parents=True)
    mapping_path.write_text(
        json.dumps([
            {"coin": "BTC", "quote": "USDT", "swap": True, "active": True, "linear": True},
            {"coin": "ETH", "quote": "USDC", "swap": True, "active": True, "linear": True},
            {"coin": "SOL", "quote": "USDT", "swap": False, "active": True, "linear": True},
            {"coin": "XRP", "quote": "USDT", "swap": True, "active": False, "linear": True},
            {"coin": "ADA", "quote": "USDT", "swap": True, "active": True, "linear": False},
        ]),
        encoding="utf-8",
    )

    class FailingCoinData:
        """Force get_market_data_coin_options through its mapping fallback."""

        def filter_mapping(self, **_kwargs):
            """Simulate an unavailable CoinData cache."""

            raise RuntimeError("cache unavailable")

    monkeypatch.setattr(market_data, "__file__", str(tmp_path / "market_data.py"))
    monkeypatch.setattr(market_data, "CoinData", FailingCoinData)
    monkeypatch.setattr(market_data, "_filter_live_market_data_coin_options", lambda _exchange, coins: coins)

    assert market_data.get_market_data_coin_options("bitget") == ["BTC"]


def test_bitget_heatmap_overview_reads_source_index() -> None:
    """Bitget remains in the source-index-backed 1m heatmap exchange set."""

    from api import heatmap

    source = inspect.getsource(heatmap.get_heatmap_overview)

    assert '"okx", "bitget"' in source


def test_bitget_minute_presence_uses_source_index(monkeypatch, tmp_path) -> None:
    """Bitget minute heatmaps retain source-index provenance."""

    import market_data

    calls: list[dict] = []
    indexed = {"20240401": {"00": {0: "api", 1: "api"}}}

    def fake_source_minutes(**kwargs):
        """Return indexed Bitget minute provenance."""

        calls.append(dict(kwargs))
        return indexed

    monkeypatch.setattr(market_data, "_resolve_dataset_coin_dirs", lambda *_args, **_kwargs: [tmp_path])
    monkeypatch.setattr(market_data, "get_source_minutes_for_range", fake_source_minutes)

    result = market_data.get_minute_presence_for_dataset("bitget", "1m", "BTC")

    assert result == {"oldest_day": "20240401", "newest_day": "20240401", "days": indexed}
    assert calls == [{"exchange": "bitget", "coin": "BTC", "start_day": None, "end_day": None}]


@pytest.mark.parametrize(
    ("target_day", "expected_stage"),
    [
        (date(2021, 8, 31), "rest_gap"),
        (date.today(), "rest_recent"),
    ],
)
def test_okx_primary_rest_failure_writes_nothing(monkeypatch, target_day: date, expected_stage: str) -> None:
    """Primary historical and recent REST ranges fail closed on any chunk error."""

    day_s = target_day.strftime("%Y-%m-%d")
    existing = {0: {"sentinel": "unchanged"}}
    writes: list[tuple] = []

    def fake_rest_fetch_range(*_args, **kwargs):
        """Return partial candles together with one exhausted chunk."""

        assert kwargs["stage"] == expected_stage
        candle = {"t": okx._day_start_ms(target_day), "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 1.0}
        return {day_s: {0: candle}}, 2, ["exhausted"]

    monkeypatch.setattr(okx, "_find_inception_ms", lambda *_args, **_kwargs: okx._day_start_ms(target_day))
    monkeypatch.setattr(okx, "_days_needing_fetch", lambda *_args, **_kwargs: [target_day])
    monkeypatch.setattr(okx, "_rest_fetch_range", fake_rest_fetch_range)
    monkeypatch.setattr(okx, "_write_candles_for_day", lambda *args, **kwargs: writes.append((args, kwargs)))
    monkeypatch.setattr(okx, "append_exchange_download_log", lambda *_args, **_kwargs: None)

    with pytest.raises(RuntimeError, match=expected_stage):
        okx.improve_best_okx_1m_for_coin(
            coin="BTC",
            start_date_override=target_day,
            end_date=target_day,
            refetch=True,
        )

    assert writes == []
    assert existing == {0: {"sentinel": "unchanged"}}


def test_okx_latest_failure_returns_error_without_writing(monkeypatch) -> None:
    """Latest refresh preserves its error result contract and rejects partial buckets."""

    writes: list[tuple] = []
    today_s = date.today().strftime("%Y-%m-%d")
    partial = {today_s: {0: {"t": okx._day_start_ms(date.today()), "v": 1.0}}}
    monkeypatch.setattr(okx, "_rest_fetch_range", lambda *_args, **_kwargs: (partial, 2, ["exhausted"]))
    monkeypatch.setattr(okx, "_write_candles_for_day", lambda *args, **kwargs: writes.append((args, kwargs)))
    monkeypatch.setattr(okx, "append_exchange_download_log", lambda *_args, **_kwargs: None)

    result = okx.update_latest_okx_1m_for_coin(coin="BTC", lookback_days=2)

    assert result["result"] == "error"
    assert result["minutes_written"] == 0
    assert "failed chunks=1" in result["error"]
    assert writes == []


def test_okx_rest_bulk_stop_cancels_queue_and_bounds_request_starts(monkeypatch) -> None:
    """REST stop cancels queued chunks and waits only for the active bounded request."""

    started = threading.Event()
    release = threading.Event()
    stopped = threading.Event()
    cancel_seen = threading.Event()
    request_starts = 0

    class RecordingExecutor(ThreadPoolExecutor):
        """Record cancellation calls made against queued REST futures."""

        def submit(self, *args, **kwargs):
            """Wrap each future's cancel method with an observable event."""

            future = super().submit(*args, **kwargs)
            original_cancel = future.cancel

            def cancel() -> bool:
                """Record and delegate cancellation."""

                cancel_seen.set()
                return original_cancel()

            future.cancel = cancel
            return future

    def fake_get(*_args, **_kwargs):
        """Hold the sole active request until queued futures are cancelled."""

        nonlocal request_starts
        request_starts += 1
        started.set()
        assert release.wait(timeout=2)
        return _FakeResponse(200, {"code": "0", "data": []})

    monkeypatch.setattr(okx, "ThreadPoolExecutor", RecordingExecutor)
    monkeypatch.setattr(okx, "REST_LIMIT", 1)
    monkeypatch.setattr(okx, "_coin_to_okx_inst_id", lambda _coin: "BTC-USDT-SWAP")
    monkeypatch.setattr(okx.requests, "get", fake_get)

    with ThreadPoolExecutor(max_workers=1) as outer:
        result_future = outer.submit(
            okx._rest_fetch_range,
            "BTC",
            0,
            10 * okx.MS_PER_MINUTE,
            workers=1,
            limiter=okx.RateLimiter(1_000_000),
            stop_check=stopped.is_set,
        )
        assert started.wait(timeout=2)
        stopped.set()
        assert cancel_seen.wait(timeout=2)
        release.set()
        buckets, pages, errors = result_future.result(timeout=2)

    assert request_starts == 1
    assert buckets == {}
    assert pages == 0
    assert errors == []


def test_okx_archive_bulk_stop_cancels_queue_and_bounds_download_starts(monkeypatch) -> None:
    """Archive stop cancels queued files without starting more downloads."""

    started = threading.Event()
    release = threading.Event()
    stopped = threading.Event()
    cancel_seen = threading.Event()
    download_starts = 0

    class RecordingExecutor(ThreadPoolExecutor):
        """Record cancellation calls made against queued archive futures."""

        def submit(self, *args, **kwargs):
            """Wrap each future's cancel method with an observable event."""

            future = super().submit(*args, **kwargs)
            original_cancel = future.cancel

            def cancel() -> bool:
                """Record and delegate cancellation."""

                cancel_seen.set()
                return original_cancel()

            future.cancel = cancel
            return future

    def fake_download(*_args, **_kwargs):
        """Hold the sole active download until queued futures are cancelled."""

        nonlocal download_starts
        download_starts += 1
        started.set()
        assert release.wait(timeout=2)
        return b"unused"

    files = [okx.ArchiveFile(filename=f"2024-01-{day:02d}.zip", url=f"https://public.invalid/{day}") for day in range(1, 11)]
    monkeypatch.setattr(okx, "ThreadPoolExecutor", RecordingExecutor)
    monkeypatch.setattr(okx, "ARCHIVE_DOWNLOAD_WORKERS", 1)
    monkeypatch.setattr(okx, "_download_bytes", fake_download)

    with ThreadPoolExecutor(max_workers=1) as outer:
        result_future = outer.submit(
            okx._download_archive_files_bulk,
            files,
            "BTC-USDT-SWAP",
            "BTC",
            skip_existing=False,
            timeout_s=1.0,
            stop_check=stopped.is_set,
        )
        assert started.wait(timeout=2)
        stopped.set()
        assert cancel_seen.wait(timeout=2)
        release.set()
        parsed, skipped, errors = result_future.result(timeout=2)

    assert download_starts == 1
    assert parsed == []
    assert skipped == 0
    assert errors == []


def test_okx_stop_after_primary_bulk_prevents_post_stop_write(monkeypatch) -> None:
    """A stop observed immediately after REST bulk return prevents all day writes."""

    stopped = threading.Event()
    writes: list[tuple] = []
    target_day = date.today()
    day_s = target_day.strftime("%Y-%m-%d")

    def fake_rest_fetch_range(*_args, **_kwargs):
        """Set stop before returning an otherwise successful bucket."""

        stopped.set()
        candle = {"t": okx._day_start_ms(target_day), "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 1.0}
        return {day_s: {0: candle}}, 1, []

    monkeypatch.setattr(okx, "_find_inception_ms", lambda *_args, **_kwargs: okx._day_start_ms(target_day))
    monkeypatch.setattr(okx, "_days_needing_fetch", lambda *_args, **_kwargs: [target_day])
    monkeypatch.setattr(okx, "_rest_fetch_range", fake_rest_fetch_range)
    monkeypatch.setattr(okx, "_write_candles_for_day", lambda *args, **kwargs: writes.append((args, kwargs)))
    monkeypatch.setattr(okx, "append_exchange_download_log", lambda *_args, **_kwargs: None)

    result = okx.improve_best_okx_1m_for_coin(
        coin="BTC",
        start_date_override=target_day,
        end_date=target_day,
        stop_check=stopped.is_set,
    )

    assert result.notes[-1] == "stopped"
    assert result.minutes_written == 0
    assert writes == []


def test_okx_archive_failure_still_reaches_rest_repair_without_redownload(monkeypatch) -> None:
    """One failed archive pass falls through to existing REST repair without redownloading."""

    target_day = date(2024, 1, 2)
    archive_file = okx.ArchiveFile(filename="2024-01-03.zip", url="https://public.invalid/archive.zip")
    bulk_calls: list[list[okx.ArchiveFile]] = []
    repair_calls: list[str] = []

    def fake_collect(*_args, **kwargs):
        """Return one monthly archive and no additional daily tail file."""

        return [archive_file] if kwargs["date_aggr_type"] == "monthly" else []

    def fake_bulk(files, *_args, **_kwargs):
        """Record one failed archive download pass."""

        bulk_calls.append(list(files))
        return [], 0, ["download failed"]

    def fake_repair(_coin, day_s, **_kwargs):
        """Record the existing REST repair fallback."""

        repair_calls.append(day_s)
        return 0, 0, okx.MIN_DAY_CANDLES

    monkeypatch.setattr(okx, "_find_inception_ms", lambda *_args, **_kwargs: okx._day_start_ms(target_day))
    monkeypatch.setattr(okx, "_coin_to_okx_inst_id", lambda _coin: "BTC-USDT-SWAP")
    monkeypatch.setattr(okx, "_coin_to_inst_family", lambda _coin: "BTC-USDT")
    monkeypatch.setattr(okx, "_collect_archive_files", fake_collect)
    monkeypatch.setattr(okx, "_download_archive_files_bulk", fake_bulk)
    monkeypatch.setattr(okx, "_read_day_npz", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(okx, "_repair_missing_minutes_for_day", fake_repair)
    monkeypatch.setattr(okx, "append_exchange_download_log", lambda *_args, **_kwargs: None)

    result = okx.improve_best_okx_1m_for_coin(
        coin="BTC",
        start_date_override=target_day,
        end_date=target_day,
    )

    assert bulk_calls == [[archive_file]]
    assert repair_calls == ["2024-01-02"]
    assert "archive_download_errors=1" in result.notes
    assert f"unrepaired_minutes=2024-01-02:{okx.MIN_DAY_CANDLES}" in result.notes
