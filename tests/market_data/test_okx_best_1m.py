"""Tests for the OKX 1m market-data downloader helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
import zipfile

import numpy as np

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
