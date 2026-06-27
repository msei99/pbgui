# OKX Market Data Integration Plan

## Goal

Add OKX USDT-SWAP 1m OHLCV downloads to PBGui Market Data with the same functional coverage as the existing Binance and Bybit 1m downloaders.

Do not omit any automatic-download, repair, retry, progress, inventory, or latest-refresh behavior that Binance/Bybit already provide. If an OKX-specific difference requires a design choice, ask before implementation instead of silently simplifying.

## Existing Reference Implementations

- `bybit_best_1m.py`
- `binance_best_1m.py`
- `task_worker.py` job runners for `bybit_best_1m` and `binance_best_1m`
- `api/market_data.py` market-data job creation and status APIs
- Frontend market-data pages under `frontend/`

## New Module

Create `okx_best_1m.py`.

Public functions must match the existing downloader pattern:

- `improve_best_okx_1m_for_coin(...)`
- `update_latest_okx_1m_for_coin(...)`
- `get_newest_day(coin)`
- `get_oldest_day(coin)`

Result dataclass:

- `ImproveBest1mOkxResult`

Suggested result fields:

- `coin`
- `end_date`
- `days_checked`
- `archive_daily_downloaded`
- `rest_minutes_fetched`
- `repair_minutes_fetched`
- `minutes_written`
- `notes`

## Storage

Use storage exchange key `okx`.

Target layout:

```text
data/ohlcv/okx/1m/<COIN>_USDT:USDT/YYYY-MM-DD.npz
```

Example:

```text
data/ohlcv/okx/1m/BTC_USDT:USDT/2024-01-01.npz
```

NPZ schema must match existing 1m market-data files:

```python
np.dtype([
    ("ts", "i8"),
    ("o",  "f4"),
    ("h",  "f4"),
    ("l",  "f4"),
    ("c",  "f4"),
    ("bv", "f4"),
])
```

The array key must be `candles`.

## OKX Endpoints

REST candles:

```text
GET https://www.okx.com/api/v5/market/history-candles
```

Params:

- `instId=BTC-USDT-SWAP`
- `bar=1m`
- `limit=300`
- `after=<timestamp_ms>` for records earlier than requested timestamp

Archive index:

```text
GET https://www.okx.com/api/v5/public/market-data-history
```

Params:

- `module=2` for 1-minute candlestick
- `instType=SWAP`
- `instFamilyList=BTC-USDT`
- `dateAggrType=daily`
- `begin=<timestamp_ms>`
- `end=<timestamp_ms>`

Archive ZIP URLs are returned in `groupDetails[].url`.

## Measured OKX Coverage

BTC-USDT-SWAP REST starts at:

```text
2019-12-16 06:09 UTC
```

ETH-USDT-SWAP REST starts at:

```text
2019-12-25 00:00 UTC
```

SWAP archive starts at:

```text
2021-09-01
```

Archive day `2021-08-31` returns no SWAP candlestick ZIP.

## Measured OKX Parallelism

Archive CDN static ZIP downloads, 120 BTC-USDT-SWAP daily ZIPs, including ZIP/CSV parse:

| Workers | Files/s | Errors |
|---:|---:|---:|
| 4 | 39.97 | 0 |
| 8 | 61.23 | 0 |
| 16 | 69.54 | 0 |
| 24 | 73.97 | 0 |
| 32 | 69.69 | 0 |
| 48 | 73.70 | 0 |

REST `history-candles`, 160 requests:

| Target Rate | Effective Rate | Result |
|---:|---:|---|
| 8 req/s | 7.93 req/s | 160 ok |
| 9 req/s | 8.91 req/s | 160 ok |
| 10 req/s | 9.88 req/s | 159 ok, 1x 429 |
| 11 req/s | 10.84 req/s | 141 ok, 19x 429 |
| 12 req/s | 11.80 req/s | 132 ok, 28x 429 |

Use these defaults:

```python
ARCHIVE_DOWNLOAD_WORKERS = 24
REST_WORKERS = 16
REST_RATE_PER_SECOND = 9.0
ARCHIVE_INDEX_RATE_PER_SECOND = 2.2
ARCHIVE_START = date(2021, 9, 1)
ARCHIVE_MIN_AGE_DAYS = 2
MAX_RETRIES = 5
RETRY_WAIT_BASE_S = 1.0
```

## Download Strategy

`improve_best_okx_1m_for_coin(...)` should:

1. Normalize coin and OKX instrument IDs.
2. Resolve end date and optional start override.
3. Find inception via OKX REST.
4. Compute planned UTC day range.
5. Scan existing NPZ files and skip complete days unless `refetch=True`.
6. Fetch REST-only gap from inception to `2021-09-01`.
7. Fetch archive index in 20-day windows, rate-limited to `2.2 req/s`.
8. Download archive ZIPs with `24` workers.
9. Parse archive CSVs and bucket rows by UTC day.
10. Merge archive rows into UTC daily NPZ files.
11. Detect missing minutes in archive-backed days.
12. Repair missing minutes via OKX REST.
13. Fetch recent/latest non-archive days via REST.
14. Update source index for written minutes.
15. Emit progress snapshots compatible with `task_worker.py`.
16. Support `stop_check` and cancellation at each long-running phase.

## OKX UTC+8 Archive Handling

OKX archive dates for module `2` use UTC+8 date boundaries. PBGui stores UTC days.

Do not write one OKX ZIP directly as one PBGui UTC NPZ day.

Required behavior:

- Parse each ZIP row by `open_time`.
- Convert each row timestamp to UTC date.
- Bucket rows by UTC date and minute index.
- Merge buckets into `YYYY-MM-DD.npz` UTC files.
- Expect each OKX archive ZIP to commonly touch two UTC days.

## Missing Minute Repair

OKX archive can miss individual minutes even when REST has them.

Observed archive omissions:

- `2022-10-26 03:01 UTC`
- `2023-06-22 22:17 UTC`
- `2023-06-22 22:18 UTC`

Required repair behavior:

1. After writing archive buckets, validate expected minute coverage for each complete UTC day.
2. Collect missing minute timestamps.
3. Fetch missing minutes via REST `history-candles`.
4. Merge returned rows into the NPZ.
5. Revalidate the day.
6. Log and report any minute that REST also cannot provide.

Repair should be automatic, not a separate manual step.

## Retry Handling

Implement clean retry handling for:

- HTTP `429`
- HTTP `5xx`
- Network errors
- Timeout errors
- Corrupt ZIP files
- CSV parse failures
- Short archive days with missing minutes

Rules:

- Use exponential backoff with jitter.
- Respect the measured REST rate limit of `9 req/s`.
- Respect archive-index rate limit of `2.2 req/s`.
- Retry failed ZIP downloads before falling back.
- If archive ZIP remains unavailable or corrupt, use REST for affected day range.
- Never silently mark incomplete data as complete.
- Add detailed `notes` and logs for unrepaired data.

## Volume Mapping

OKX fields:

- `vol`: contract volume for SWAP
- `volCcy` / `vol_ccy`: trading volume with unit `currency`
- `volCcyQuote` / `vol_quote`: trading volume with unit quote currency

Verified behavior from live OKX data:

- REST `history-candles` returns `volCcy` and `volCcyQuote` for BTC/ETH SWAP back to at least 2020.
- Archive ZIPs from 2021-2023 often contain `vol_ccy=None` and `vol_quote=None` even when `vol` is present.
- Archive ZIPs from 2024 onward contain usable `vol_ccy` and `vol_quote`.
- `vol` is not equivalent to base volume. It is contract volume.

PBGui `bv` mapping rule:

1. Store `volCcy` / `vol_ccy` in `bv` whenever present and numeric.
2. For archive rows where `vol_ccy` is missing, fetch the same candles via REST and use REST `volCcy`.
3. If REST does not return a candle but OHLC data exists in archive, compute base volume from OKX contract size as a last-resort fallback and add a result note.
4. Do not silently store raw OKX `vol` as `bv` unless the instrument contract size conversion has been applied.

Known BTC/ETH linear SWAP examples from REST:

- BTC-USDT-SWAP: `vol=155`, `volCcy=0.0155`, contract size factor `0.0001 BTC/contract`.
- ETH-USDT-SWAP: `vol=72323`, `volCcy=72.323`, contract size factor `0.001 ETH/contract`.

Implementation should prefer REST enrichment over hardcoded assumptions. Contract-size fallback should derive size from OKX instrument metadata (`GET /api/v5/public/instruments`) and only be used when REST enrichment fails.

## Latest Refresh

Implement `update_latest_okx_1m_for_coin(...)` analogous to Bybit/Binance:

- Args: `coin`, `lookback_days=DEFAULT_LATEST_LOOKBACK_DAYS`, `overwrite=True`, `timeout_s=30.0`.
- Use REST only.
- Rate limit to `9 req/s`.
- Merge rows into daily NPZ.
- Overwrite latest lookback days if requested.
- Return dict with:
  - `coin`
  - `lookback_days`
  - `pages` or `chunks`
  - `days_fetched`
  - `minutes_written`
  - `repair_minutes_fetched` if any
  - `result`

## Convenience Helpers

Implement helpers analogous to Bybit/Binance:

- `_coin_to_okx_inst_id(coin)` -> `BTC-USDT-SWAP`
- `_coin_to_inst_family(coin)` -> `BTC-USDT`
- `_coin_dir(coin)` -> `BTC_USDT:USDT`
- `_okx_day_path(coin, day)`
- `_list_existing_days(coin)`
- `_read_day_npz(path, day=...)`
- `_write_day_npz(path, candles_by_minute)`
- `_write_candles_for_day(coin, day, candles, overwrite=False, source_code=...)`
- `_find_inception_ms(coin, timeout_s=...)`
- `_collect_archive_files(...)`
- `_download_archive_files_bulk(...)`
- `_parse_archive_zip(...)`
- `_rest_fetch_range(...)`
- `_repair_missing_minutes(...)`
- `_validate_day_minutes(...)`

## Source Index

Call `update_source_index_for_day(...)` for all written minutes.

Use source codes from `market_data_sources.py`:

- Archive minutes should use an archive/source code if available.
- REST/API minutes should use `SOURCE_CODE_API`.

If there is no existing source code for OKX archive, add one in `market_data_sources.py` and update tests/docs accordingly.

## Task Worker Integration

Update `task_worker.py`:

- Import `improve_best_okx_1m_for_coin`.
- Add dispatch for `jtype == "okx_best_1m"`.
- Add `_run_okx_best_1m(job_path, payload)`.
- Mirror `_run_binance_best_1m` / `_run_bybit_best_1m` behavior:
  - coins list
  - start/end day
  - refetch
  - progress snapshots
  - job log heartbeats
  - cancellation via `_is_cancel_requested`
  - final result logging
  - inventory refresh

Progress mode:

```text
okx_best_1m
```

Exchange log key:

```text
okx
```

Inventory refresh:

```python
_refresh_inventory_coin("okx", "1m", coin)
```

## API Integration

Update `api/market_data.py`:

- Allow OKX in exchange list for 1m OHLCV jobs.
- Create `okx_best_1m` job payloads.
- Ensure status/progress response handles OKX mode.
- Include OKX in any exchange flag/status helpers if needed.
- Preserve existing Binance/Bybit/Hyperliquid behavior.

Because `api/market_data.py` changes, increment `api/serial.txt` by 1.

Before modifying API route handlers, run `gitnexus_api_impact` for affected routes.

## Frontend Integration

Update Market Data frontend pages if OKX is currently missing:

- `frontend/market_data_main.html`
- `frontend/market_data_status.html`
- any shared JS if exchange lists are hardcoded

Requirements:

- OKX appears alongside Binance, Bybit, Hyperliquid.
- Manual backfill job can be started for OKX.
- Auto-download controls include OKX if present for other exchanges.
- Progress/status UI displays `okx_best_1m` mode cleanly.
- Increment `?v=N` cache-busting versions for changed frontend assets.

## Market Data Config / Auto Downloads

OKX must be included wherever Binance/Bybit are included for automatic 1m downloads:

- enabled coins config
- auto-enable-new-coins behavior
- job queue creation
- status aggregation
- inventory cache refresh
- latest refresh scheduling, if Binance/Bybit have it

Do not skip background/automatic download integration.

## Tests

Add tests covering:

- OKX instrument normalization (`BTC` -> `BTC-USDT-SWAP`, `BTC_USDT:USDT`).
- Archive parser splits UTC+8 archive rows into UTC days.
- NPZ dtype and `candles` key.
- Missing minute detection.
- REST repair fills archive gaps.
- Retry behavior for `429`.
- Retry behavior for transient network/5xx errors.
- Task-worker dispatch for `okx_best_1m`.
- API job creation for OKX.

Manual smoke tests:

- BTC small archive range: `2021-09-01..2021-09-03`.
- BTC full range if time allows.
- ETH small range.

## Verification

Run targeted tests first, then full tests if safe:

```bash
/home/mani/software/venv_pbgui/bin/python -m pytest tests/
```

Do not run destructive tests or mutate real runtime data except through the intended temporary/test paths.

## Changelog And Serial

For implementation:

- Add/update `releases/unreleased.md`.
- Increment `api/serial.txt` if any `api/`, `PBApiServer.py`, or API startup-imported module changes.
- Ask before committing or pushing.

## Temporary Probe Artifacts

Temporary scripts used during planning:

- `/tmp/opencode/okx_btc_1m_npz_test.py`
- `/tmp/opencode/okx_parallel_probe.py`

Temporary output from BTC test:

```text
/tmp/opencode/okx_btc_1m_npz/data/ohlcv/okx/1m/BTC_USDT:USDT
```

These are not part of the repo implementation unless explicitly copied into tests or fixtures.
