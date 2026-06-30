# Bitget Market Data Integration Plan

_Last updated: 2026-06-29 (rev 5) — aligned implementation defaults with official Bitget endpoint limits: `history-candles` and `candles` are documented as `20 times/1s (IP)`, so PBGui uses `18 req/s` with shared 429 penalty backoff._

## Goal

Add Bitget USDT-FUTURES 1m OHLCV downloads to PBGui Market Data with the same
functional coverage as the existing Binance, Bybit, OKX, and Hyperliquid 1m
downloaders.

Bitget is **REST-only** (no public historical ZIP archive) and behaves most
like the existing `bybit_best_1m.py` strategy: pre-built candles via REST with
backward pagination. It does **not** mirror the OKX archive-ZIP download path.

## Integration Touch-Point Index

The implementation touches **12 files** outside the new downloader module.
This index is the authoritative checklist — every entry here must be edited
or it ships broken. Each is detailed in the sections below.

| # | File | Change | Plan section |
|---|---|---|---|
| 1 | `bitget_best_1m.py` (new) | New module — REST loop, inception probe, NPZ writer | New Module, Download Strategy, Latest Refresh |
| 2 | `task_worker.py` | Import + `jtype` branch + `_run_bitget_best_1m` | Market Data Config / Auto Downloads |
| 3 | `PBData.py` | Import + state attrs + `_load_settings` block + `_bitget_latest_1m_loop` + task creation | Market Data Config / Auto Downloads |
| 4 | `market_data.py` | `quote_filter` add `bitget` (USDT only) | Market Data Config / Auto Downloads |
| 5 | `api/market_data.py` | `SETTINGS_EXCHANGES`, `BEST_1M_EXCHANGES`, `COPY_DATA_EXCHANGES`, `_get_exchange_status_key`, `_get_exchange_flag_prefix` | Market Data Config / Auto Downloads |
| 6 | `api/heatmap.py` | 1m source-count tuple (`:288`), `_get_missing_lag_minutes` (`:42`) | Market Data Config / Auto Downloads |
| 7 | `api/jobs.py` | Legacy deletion-by-exchange fallback (`:177-182`) | Market Data Config / Auto Downloads |
| 8 | `frontend/market_data_main.html` | Dropdown (`:2740`), `exchangeOptions` (`:3236`), `best1mJobMonitorMeta` (`:3287`), `best1mQueueMeta` (`:3292`), Copy-Data checkboxes (`:3088`), `?v=N` bump | Market Data Config / Auto Downloads |
| 9 | `frontend/jobs_monitor.html` | Legacy exchange detection (`:607-615`) | Market Data Config / Auto Downloads |
| 10 | `market_data_sources.py` | No change needed — `SOURCE_CODE_API` covers REST | Source Index |
| 11 | `inventory_cache.py` | No change needed — exchange-neutral | Market Data Config / Auto Downloads |
| 12 | `pbgui.ini.example` | Add `[bitget_data]` section documentation | Market Data Config / Auto Downloads |
| — | `api/serial.txt` | Bump by 1 (touches `api/` modules) | Changelog And Serial |
| — | `releases/unreleased.md` | Implementation entry | Changelog And Serial |

> A prior version of this plan called the auto-download section "hide it under
> OKX's umbrella"; that was wrong. PBGui has **one asyncio loop per exchange**,
> not one keyed by a config map. Each addition requires cloning a ~120-line
> block. This index reflects that.

## Probe Summary (Already Measured)

Two temporary probe scripts were used to verify feasibility, speed, and data
quality:

- `/tmp/opencode/bitget_btc_1m_npz_test.py`  — original urllib-based probe
- `/tmp/opencode/bitget_btc_1m_npz_v2.py`    — improved probe (`requests.Session`
  per worker, connection pool, silent retry on 429) — **this is the shape the
  production module should adopt**
- `/tmp/opencode/bitget_vs_binance_quality.py` — quality comparator (Bitget vs
  Binance/Bybit reference stored in `data/ohlcv/`)

All measurements below are from live BTCUSDT probes.

### Bitget v2 REST endpoints

| Endpoint | Path | Purpose |
|---|---|---|
| Recent candles | `GET /api/v2/mix/market/candles` | Most recent 200 candles, ascending. Used for `update_latest`. |
| History candles | `GET /api/v2/mix/market/history-candles` | Older candles, paginated backward via `endTime`. Used for backfill. |

Base URL: `https://api.bitget.com`

### Required query params

- `symbol=BTCUSDT` (Bitget native symbol, as stored in `data/coindata/bitget/mapping.json` field `symbol`)
- `productType=USDT-FUTURES` (for USDT-margined linear perps)
- `granularity=1m` — **strictly `1m`**, NOT `1min`. Bitget rejects other forms
  with `400171` "k-line time range should be [1m,3m,5m,15m,30m,1H,4H,6H,12H,1D,1W,1M,...]"
- `limit` — hard max **200**. Bitget returns `40053` "limit should be between 1~200"
  for any value above 200.
- `endTime=<ms>` — returns candles strictly older than `endTime`, ascending order.
  Use the first timestamp of the previous batch as the next batch's `endTime` to
  paginate backward.
- `startTime=<ms>` (optional) — paired with `endTime` returns the latest N candles
  in that window. Backward `endTime` pagination is the recommended strategy.

### Response row shape

```text
[ts_ms, open, high, low, close, base_vol, quote_vol]
```

Verified mapping:

- Index 0: `ts_ms` (open time, milliseconds)
- Index 1..4: `o, h, l, c` (price strings)
- Index 5: `base volume` (e.g. BTC) — store as PBGui `bv`
- Index 6: `quote volume` (e.g. USDT)

Verification: close=35531.8 × base_vol=27.59 ≈ quote_vol 980790.4788. Bitget
returns base volume directly, so **no contract-size conversion is needed**
(simpler than OKX, whose archive needs `vol_cecy` derivation from `ct_val`).

### Measured coverage

- **Inception** of BTCUSDT USDT-FUTURES 1m: `2019-07-10 11:49 UTC`
  (`1562759340000`). First day is partial (`731` of `1440` possible minutes —
  trading opened mid-day). This is the only partial day observed; v2 full
  backfill produced 2545 NPZ files with **0 interior incomplete days**
  (where interior = not the inception day, not today, not `refetch=True`).
- Modern full days deliver exactly `1440/1440` minutes with zero observed gaps
  (30-day 2024-04 backfill produced 30×1440 = 43200 rows, all complete).
- Because REST is the only source, any missing minutes in Bitget REST means
  Bitget does not have those candles at all — no alternate source to repair from.

### Measured rate limits / parallelism

Two measurement rounds were run:

**Round 1 — bare urllib, no retry** (160-200 request bursts). Establishes the raw ceiling before retries mask it.

| Target Rate | Effective | 429s | Notes |
|---:|---:|---:|---|
| 10 req/s | 9.84 req/s | 0 | safe |
| 20 req/s | 19.23 req/s | 0 | safe |
| 25 req/s | 23.81 req/s | 0 | safe |
| 30 req/s | 28.02 req/s | 0 | safe — raw ceiling |
| 40 req/s | 36.76 req/s | ~3.7% | noticeable |
| 50 req/s | 44.78 req/s | ~10% | too high |
| 60+ req/s | — | large 429 count | not useful |

**Round 2 — `requests.Session` per worker + silent retry on 429** (500-request bursts). This was the first production-shape configuration probe.

| Target Rate | Effective | Net 429s | Result |
|---:|---:|---:|---|
| 30 req/s | 28.65 req/s | 0 (all absorbed) | 500/500 ok |
| 40 req/s | 35.60 req/s | 0 (all absorbed) | 500/500 ok in short burst |
| 50 req/s | 39.57 req/s | 0 (all absorbed) | 500/500 ok |

Short 40 req/s bursts looked acceptable, but longer real jobs later produced
HTTP 429 failures. Do **not** use 40 req/s as the default without a longer
soak test.

**Round 3 — live ATOMUSDT history-candles re-measurement after job failures**
(`2026-06-29`, same v2 endpoint/payload shape as production, 24 worker sessions,
stop on first 429-bearing rate):

| Target Rate | Effective | 429s | Notes |
|---:|---:|---:|---|
| 28 req/s | 27.76 req/s | 31 / 1273 | measured while Bitget/IP was still hot after a failed 40-rps job |
| 12 req/s | 11.90 req/s | 0 / 381 | clean after 90s cooldown |
| 16 req/s | 15.87 req/s | 1 / 499 | occasional 429 after cooldown |
| 20 req/s | 19.85 req/s | 9 / 917 | still 429 after 3m cooldown |

Round 3 shows the public endpoint can remain temporarily throttled after bursts.
The official Bitget docs for both `/api/v2/mix/market/history-candles` and
`/api/v2/mix/market/candles` document `20 times/1s (IP)`, so the implementation
must stay below that endpoint limit. PBGui uses `18 req/s` to leave headroom for
other Bitget calls from the same IP and adds shared 429 penalty backoff across
threads. A single hot window must slow the job down, not permanently fail it.

Recommended operating defaults (Session + silent-retry shape):

```python
REST_LIMIT = 200              # Bitget hard max per request
REST_RATE_PER_SECOND = 18.0   # below official 20 times/1s (IP) endpoint limit
REST_WORKERS = 16
REST_POOL_CONNECTIONS = 2     # per-worker HTTPAdapter pool_connections
REST_POOL_MAXSIZE = 8         # per-worker HTTPAdapter pool_maxsize
ARCHIVE_DOWNLOAD_WORKERS = 0  # Bitget has no archive
MAX_RETRIES = 8               # retries per request before surfacing
RETRY_WAIT_BASE_S = 0.5
RETRY_WAIT_MULT = 2.0
RETRY_WAIT_MAX_S = 20.0
RATE_LIMIT_PENALTY_S = 3.0    # shared limiter penalty on 429
DEFAULT_LATEST_LOOKBACK_DAYS = 3
MIN_DAY_CANDLES = 1440
```

Inception default anchor for binary probing:

```python
INCEPTION_PROBE_LOW = datetime(2018, 1, 1, tzinfo=timezone.utc)
INCEPTION_DEFAULT   = date(2019, 7, 10)  # BTC USDT-FUTURES first candle day
```

### Measured download speeds

Two full BTC backfills (2019-07-10 inception → 2026-06-27, 2545 days, 3.66M
rows, 18.3k chunks) were run end-to-end to validate the speed-up:

| Variant | HTTP client | Wall time | Effective | Errors | 429s |
|---|---|---:|---:|---:|---:|
| v1 (urllib, no retry) | `urllib.request` per request | **738.95 s** (~12min 19s) | 25.23 req/s | 0 | 0 |
| v2 (Session+retry) | `requests.Session` per worker | **518.84 s** (~8min 39s) | ~35 req/s | 0 | 0 (absorbed) |

Speedup: **−29.8 %** (738.95 s → 518.84 s) with identical data quality (same
3,664,091 rows, 2545 NPZ files, 0 interior incomplete days, same inception-day
partial of 731/1440). The TLS-handshake cost per request is the dominant
overhead v1 pays; v2 amortizes it via the per-worker connection pool.

Short-range runs for reference:

| Range | Days | Chunks | Rows | Wall time |
|---|---:|---:|---:|---:|
| 2024-04-01..2024-04-03 (v2) | 3 | 22 | 4400 | 1.17 s |
| 2024-04-01..2024-04-30 (v1) | 30 | 216 | 43200 | 34.32 s |

Even worst-case wall-clock for a single-coin full BTC history is small (~9 min),
so an OKX-style archive ZIP downloader is **not required** for Bitget.

### Why not faster than ~9 min / coin

| Hebel | Status | Reason |
|---|---|---|
| Granularität > 1m | ❌ | 1m OHLCV nicht aus größeren Kerzen rekonstruierbar (O/H/L/C anders) |
| `limit` > 200 | ❌ | Bitget hard max `40053` |
| Bitget Archiv (ZIPs wie OKX) | ❌ | existiert nicht |
| WebSocket-Streams | ❌ | nur live, keine Historie |
| Mehrere IPs / Clients | ⚠️ | bringt ~2x, aber Komplexität + ToS-Risiko |
| gzip-Accept-Encoding | ❌ | Bitget antwortet nicht mit gzip (Responses nur ~126 Bytes/Zeile) |

Realistische Obergrenze für Single-Coin-Full-Backfill ist ~7–9 Min.

## Existing Reference Implementations

- `bybit_best_1m.py` — closest analog: REST-only, pre-built candles,
  concurrent CCXT-style fetch, backward pagination.
- `okx_best_1m.py` — reference for archive + REST hybrid, UTC-day bucketing,
  source index, result dataclass, `task_worker` integration shape.
- `binance_best_1m.py` — reference for daily NPZ write + source index.
- `task_worker.py` — job runners for `bybit_best_1m`, `binance_best_1m`,
  `okx_best_1m`.
- `api/market_data.py` — market-data job creation and status APIs.
- Frontend market-data pages under `frontend/`.

## New Module

Create `bitget_best_1m.py`.

Public functions must match the existing downloader pattern:

- `improve_best_bitget_1m_for_coin(...)`
- `update_latest_bitget_1m_for_coin(...)`
- `get_newest_day(coin)`
- `get_oldest_day(coin)`

Result dataclass:

- `ImproveBest1mBitgetResult`

Suggested result fields (mirror OKX minus archive-only fields):

- `coin`
- `end_date`
- `days_checked`
- `rest_minutes_fetched`
- `repair_minutes_fetched`
- `minutes_written`
- `notes`

(`archive_daily_downloaded` from OKX is intentionally omitted — Bitget has no
archive. Keep the field set in parity with Bybit/Binance where appropriate. If
other downloaders omit it, omit here too; otherwise keep a 0-filled field for
caller/UI uniformity.)

## Storage

Use storage exchange key `bitget`. No alias/symlink is needed — `_EXCHANGE_ALIASES`
(`market_data.py:74`) only maps `binance → binanceusdm` to satisfy PB7's
name-stripping behaviour; Bitget stores directly under `bitget/`.

Target layout:

```text
data/ohlcv/bitget/1m/<COIN_DIR>/YYYY-MM-DD.npz
data/ohlcv/bitget/1m_src/<COIN_DIR>/sources.idx
```

`COIN_DIR` format mirrors PB7 cache layout, e.g. `BTC_USDT:USDT`,
`1000PEPE_USDT:USDT`. Determined from `data/coindata/bitget/mapping.json` field
`ccxt_symbol` (e.g. `BTC/USDT:USDT`) by splitting base currency; prefix
`1000` is required for sub-1 USD coins (Bitget's `1000PEPEUSDT` →
`1000PEPE_USDT:USDT`).

### Coin-dir normalization point

`market_data.normalize_market_data_coin_dir(exchange, coin)` only special-cases
Hyperliquid; for every other exchange it returns `raw.upper()` — which means the
storage directory `BTC_USDT:USDT` must be computed **inside the downloader**, not
by the generic normalizer. The OKX downloader already does this: it exposes
`get_storage_coin_dir(coin)` (`okx_best_1m.py:335`) and the API/inventory
codepaths mostly stay out of the way because they read directories via
`inventory_cache._coin_dirs_for_dataset`, which `os.scandir`s whatever exists on
disk.

Production Bitget module must do the same:

- Implement private `_coin_dir(coin) -> str` returning `BTC_USDT:USDT` (base
  resolved from `bitget/mapping.json`, prefix `1000` applied, `_USDT:USDT`
  suffix appended).
- Expose a **public** `get_storage_coin_dir(coin) -> str` mirror so that any
  API code that needs to compute the storage directory for a coin (e.g. for
  status checks, copy-data paths) can resolve it without ad-hoc string math.
  This mirrors OKX and keeps the Bitget UI consistent.
- Do **not** extend `normalize_market_data_coin_dir` to know about Bitget. The
  function is intentionally generic; per-exchange dir derivation belongs in
  the downloader module per existing pattern.

NPZ schema must match existing 1m market-data files exactly:

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

The array key must be `candles`. Atomic write via temp file + `os.replace()`.

### `get_newest_day` / `get_oldest_day` return format

PBData's per-exchange loops (`PBData.py:1301-1304`) read
`datetime.strptime(newest_day, "%Y%m%d")` from
`bitget_get_newest_day(coin)`. The Bitget module's `get_newest_day(coin)` and
`get_oldest_day(coin)` MUST return `YYYYMMDD` (e.g. `20240401`), matching Bybit
and OKX (`bybit_best_1m.py:780-789`, `okx_best_1m.py:get_newest_day`). Files
on disk are named `YYYY-MM-DD.npz`, so the helpers use
`path.stem.replace("-", "")` or `date.strftime("%Y%m%d")` before returning.

## Symbol Resolution

Use `data/coindata/bitget/mapping.json` (already present, 663 USDT linear perps):

- Filter records where `quote == "USDT"` and `swap == True` and `active == True`.
- Native Bitget symbol is the `symbol` field directly (e.g. `BTCUSDT`).
- Product type is `USDT-FUTURES` for all USDT-quote swap records.

Do **not** perform ad-hoc CCXT market fetches for symbol resolution — `mapping.json`
is the single source of truth (per AGENTS.md symbol resolution rule).

Helpers analogous to OKX:

- `_coin_to_bitget_symbol(coin)` -> `BTCUSDT`
- `_coin_dir(coin)` -> `BTC_USDT:USDT`
- `_bitget_day_path(coin, day)`
- `_load_bitget_usdt_map()` (memoized by `mapping.json` mtime+size, like OKX)

Coin input should accept: `BTC`, `BTCUSDT`, `BTC/USDT:USDT`, `BTC_USDT:USDT`.

## Download Strategy

`improve_best_bitget_1m_for_coin(...)` should:

1. Normalize coin and Bitget native symbol.
2. Resolve end date and optional start override.
3. Find inception via Bitget REST using a binary probe on `history-candles`
   (analogous to OKX `_find_inception_ms`, querying `limit=1` and walking via
   `endTime`).
4. Compute planned UTC day range.
5. Scan existing NPZ files and skip complete days unless `refetch=True`.
6. Backfill via REST using backward `endTime` pagination in chunks of 200
   minutes, concurrently with `REST_WORKERS` workers and a `REST_RATE_PER_SECOND`
   limiter (analogous to OKX `_rest_fetch_range`).
7. Bucket returned rows by UTC day and minute index (Bitget returns ascending
   rows; rows landing outside the requested window are still acceptable —
   do not write minutes older than `start` unless the day overlaps inception).
8. Merge buckets into UTC daily NPZ files (atomic temp + `os.replace`).
9. Validate expected minute coverage for each complete UTC day; collect any
   missing minutes and re-fetch via REST for that exact UTC day window only.
   If REST still cannot supply the minutes, log and add a `notes` entry
   (`unrepaired_minutes=<day>:<count>`) — do not silently mark incomplete.
10. Fetch recent/latest days via REST `candles` endpoint (most recent batches)
    for `update_latest_bitget_1m_for_coin`.
11. Update source index for written minutes.
12. Emit progress snapshots compatible with `task_worker.py`.
13. Support `stop_check` and cancellation at each long-running phase.

### Backward pagination detail

Each `history-candles` request with `endTime=T` returns up to 200 ascending
rows with `ts < T`. To continue backward, set the next request's `endTime` to
the `ts` of the first row returned by the current request (i.e., the oldest
row in the batch). Stop when the oldest row's `ts < since_ms` or when the API
returns an empty `data` array.

**Build the cursor list up-front by splitting `[since_ms, end_ms)` into fixed
200-minute steps** (`endTime=end_ms`, `endTime=end_ms-200min`, ...). Submit all
cursors to a thread pool; the shared `RateLimiter` bounds throughput.

**Important — cursors are NOT strictly non-overlapping windows.** Bitget may
return rows older than the previous cursor's window when data is sparse (e.g.
low-liquidity minutes with no candle, the API returns the next available older
ts). Therefore two adjacent cursors can return rows for the same minute, and
the production code MUST deduplicate:

- Bucket candles by **UTC day → minute index (0..1439)** dict-of-dicts as the
  primary key (the same shape OKX uses). The v2 probe already does this:
  `buckets.setdefault(day, {})[idx] = candle`. A later write for the same
  `(day, idx)` overwrites equal-timestamp candles idempotently — that is the
  dedupe.
- Do **not** filter rows by `chunk_start <= ts < chunk_end`. Accept all rows
  returned and bucket them; the last-write-wins dict update makes overlap safe.
- When writing/final-bucketing the requested range, still enforce the global
  range boundary `since_ms <= ts < end_ms`. This preserves `start_date_override`
  semantics while avoiding unsafe per-chunk trimming.
- For the inception boundary day, when writing the NPZ, drop any candle whose
  `ts < inception_ms` (Bitget returns `ts >= inception_ms` always, so this is
  defensive only).
- For the `end_date` boundary day, if it equals today, leave it partial — do
  not pad with zero-volume candles and do not mark it as `unrepaired`.

A clean alternative (avoid the overlap entirely) is to use `startTime` +
`endTime` pairs per cursor. Bitget supports both params: a request with both
returns the latest N rows within the window. Use this only if the simpler
`endTime-only` shape produces measurable cross-window duplicates of
non-equal-timestamp rows in production (which the probe did not observe).

### Recent candles for `update_latest`

The recent `candles` endpoint returns the most recent 200 ascending rows for the
symbol with no `endTime`/`startTime` params. Use this in
`update_latest_bitget_1m_for_coin`:

- Request the most recent page directly.
- Use `history-candles` with `endTime=now` to backfill the lookback window.
- Overwrite the lookback lookback days (`overwrite=True`).
- Return the same dict shape as OKX/Binance latest refresh.

## HTTP Client Shape

The production module must use the **v2 probe shape** (the v1 urllib shape is
the bottleneck, not a reference):

- One `requests.Session` **per worker thread**, mounted with an `HTTPAdapter`
  configured with `pool_connections=2, pool_maxsize=8, max_retries=0`. The
  per-thread session is created lazily via `threading.local()` so workers do
  not share a session across threads.
- Rely on the session's keep-alive connection pool so each of the ~18k request
  calls per full coin backfill reuses an already-open TLS connection rather
  than paying the handshake every time.
- Shared process-wide `RateLimiter` (the existing OKX-style lock + monotonic
  clock) bounds throughput to `REST_RATE_PER_SECOND`.
- Do **not** set `Accept-Encoding: gzip` — Bitget already returns small JSON
  bodies (~126 bytes/row × 200 rows ≈ 25 KB/page) and does not honour gzip on
  the candle endpoints, so it adds no win.

## Retry Handling

Implement clean retry handling for:

- HTTP `429` (Bitget throttles). Bitget returns 429 both as HTTP status 429 and
  in the JSON body as code `429xx`.
- HTTP `5xx`
- Network / timeout errors
- JSON parse failures
- `code != "00000"` API error responses (e.g. transient `30014`, `30015`)

Rules:

- **Retry** up to `MAX_RETRIES = 8` per request before surfacing a failure.
  HTTP/API 429 responses must also call the shared limiter penalty so all worker
  threads slow down together instead of retrying independently into the same
  throttle window.
- Exponential backoff with jitter: `RETRY_WAIT_BASE_S = 0.5`, multiplier 2.0,
  capped by `RETRY_WAIT_MAX_S = 20.0`. On 429, apply at least
  `RATE_LIMIT_PENALTY_S = 3.0` seconds.
- `code` starting with `400` (e.g. `40053` limit-out-of-range, `400171`
  bad granularity) is a programming error and must **not** be retried — surface
  it as a failed page so a bug is not silently swallowed.
- `code` starting with `429` or `30014` / `30015` is retried.
- Never silently mark incomplete data as complete.
- Add detailed `notes` and logs for unrepaired minutes and any request that
  exhausted `MAX_RETRIES`.
- **Use `append_exchange_download_log("bitget", line, level=...)`** (same as
  OKX/Bybit/Binance) rather than calling `_log('BitgetBest1m', ...)` directly.
  The exchange-download log helper routes everything through the existing
  `MarketData` service via `human_log("MarketData", ...)` (`market_data.py:245`)
  with tags like `market_data` and `ex:bitget`, so entries appear in the
  central MarketData log stream. Do **NOT** add a `BitgetBest1m` service to
  `LOG_GROUPS` — none of the other `*_best_1m` modules use a private service
  key. Keep parity.

## Backend top-level error codes known

- `00000` success
- `40053` param validation (e.g. limit > 200) — treat as programming error,
  but log and raise rather than retry forever
- `429xx` / HTTP 429 — rate limit, retry
- `30014` / `30015` throttling — retry
- `400171` invalid granularity — programming error, do not retry

## Volume Mapping

Bitget returns base volume directly in `data[5]`. **Store `data[5]` as `bv`**.
No `vol_ccy` derivation, no contract-size conversion, no REST enrichment step
(unlike OKX). Drop the OKX `_enrich_missing_archive_volumes_*` phase entirely.

Bitget's documented contract size for BTCUSDT USDT-FUTURES is `1` (1 USD face
value per contract was the v1 layout; v2 returns `base volume` already scaled
to the base currency). The probe verified: `close=59677 × base_vol=26.3322 ≈
quote_vol 1571095.8`, so `data[5]` is genuine base-currency volume. **No
`_contract_volume_to_base` helper is needed.**

## Daily Completeness Threshold

Bybit uses `MIN_DAY_CANDLES = 1380` (95 % of 1440, accounting for Bybit trade
aggregation gaps in low-liquidity minutes). OKX uses `1440`. **Bitget must use
`MIN_DAY_CANDLES = 1440`** because the probe showed zero gaps in modern BTC
perp data (28-day sampled windows had 1440/1440 every day). The threshold
governs three things; each must consider the boundary exceptions:

1. **Skip-existing check** (`_is_day_complete_on_disk`) — a day with
   `< 1440` minutes is refetched unless explicitly exempt. The following days
   are **always exempt**:
   - Inception day (`inception_day` derived from `_find_inception_ms`)
   - Today (UTC `date.today()`, may be partial because we are mid-day)
   - The configured `end_date` if it is today
2. **Repair loop** — iterates only `_iter_day_range(archive_start, archive_end)`
   days and skips completed ones. For Bitget there is no archive range; iterate
   `[d_start, d_end - 1]` and skip days meeting the threshold. Always skip
   inception day and today.
3. **Source-index `unrepaired_minutes` note** — for any day still missing
   minutes after repair attempts that is **not** the inception day and **not**
   today, log `unrepaired_minutes=<day>:<count>` (per OKX pattern), not for
   exempt days.

Failure to exempt the inception day here will cause the repair loop to refetch
the inception day's 731 minutes forever (Bitget has no more to give). Failure
to exempt today will trigger the same issue every run while the day is open.

## Data Quality Verification (Bitget vs Binance / Bybit)

A comparison script (`/tmp/opencode/bitget_vs_binance_quality.py`) pitted the
v2 Bitget download against the production Binance (`data/ohlcv/binance/`) and
Bybit (`data/ohlcv/bybit/`) reference BTC 1m files, across 28 sample days from
2019-10-15 through 2026-04-15 (one per quarter).

Findings:

- **Timestamp alignment**: At every sampled day, all 1440 of 1440 minute
  timestamps match the Binance reference **exactly**. An auto-shift probe
  (testing ±3-minute shifts) found `shift=0` is optimal everywhere — there is
  no offset between the Bitget clock and the Binance clock.
- **Missing minutes**: 0 minutes missing on either side at every sampled day.
  Bitget BTC USDT-FUTURES is gap-free from 2019-10 onward except the inception
  day (731/1440 — Bitget opened mid-day).
- **Price (close/open/high/low)**: Median relative close error 0.0001 %–0.06 %,
  average 0.002 %–0.068 %, max 0.025 %–0.41 %. These are normal
  inter-exchange arbitrage spreads, not bugs. All four OHLC fields track to the
  same tolerance.
- **Byte-level integrity**: Direct API re-fetch of the 2021-07-20 anomaly
  minutes returns exactly the same `base_vol` bytes that the v2 download
  stored (e.g. 0.084 BTC for minute 1626739320000), so the stored values are
  not a parser bug — they are what Bitget reports.
- **Volume**: Bitget `bv` is generally smaller than Binance `bv` (Bitget is a
  smaller exchange). Ratios are reasonable (median 0.24×–1.77×) for most of
  2020–2026, with one notable exception: Mid-2021 (May–Oct, after the China
  mining crackdown) where Bitget BTC perp liquidity collapsed and Bitget
  reports 0.001×–0.15× of Binance volume. This is real Bitget liquidity, not
  a data error — confirmed by comparing stored bytes against today's live API.

Conclusion: the Bitget BTC USDT-FUTURES 1m data is usable as-is. Implement a
periodic re-validation in the production module (compare inventory counts and
spot-check prices against other exchanges if available; log divergence over a
threshold rather than overwrite from another exchange — each exchange is its
own source of truth).

## Missing Minute Repair

Bitget REST has no observed gaps in modern BTC USDT-FUTURES data (verified
30-day window with all 1440/1440 days complete). The implementation should still:

1. After backfill, validate expected minute coverage (0..1439) for each
   complete UTC day except the inception day and today/current partial day.
   A historical `end_date` is a complete UTC day and must still be validated.
2. For any day missing minutes, issue a targeted `history-candles` request with
   `startTime`/`endTime` covering that UTC day (1440-minute window, `limit=200`
   means up to 8 paginated calls per day).
3. Merge and re-validate. If still missing, log and add a `notes` entry —
   do not retry forever (REST is the source of truth, gaps are real).

Behavior parity note: unlike OKX, which has archive omissions that REST can
repair, Bitget gaps are pure REST gaps and cannot be "repaired from another
source". The repair step here is best-effort re-fetch only.

## Latest Refresh

Implement `update_latest_bitget_1m_for_coin(...)` analogous to
Bybit/Binance/OKX:

- Args: `coin`, `lookback_days=DEFAULT_LATEST_LOOKBACK_DAYS`, `overwrite=True`,
  `timeout_s=30.0`.
- Use REST only (recent `candles` + `history-candles`).
- Rate limit to `18 req/s` with shared 429 penalty backoff (see HTTP Client Shape).
- Merge rows into daily NPZ (atomic write).
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

Implement helpers analogous to OKX/Bybit:

- `_coin_to_bitget_symbol(coin)` -> `BTCUSDT`
- `_coin_dir(coin)` -> `BTC_USDT:USDT`
- `_bitget_day_path(coin, day)`
- `_list_existing_days(coin)`
- `_read_day_npz(path, day=...)`
- `_write_day_npz(path, candles_by_minute)`
- `_write_candles_for_day(coin, day, candles, overwrite=False, source_code=...)`
- `_find_inception_ms(coin, timeout_s=...)`
- `_rest_fetch_range(coin, since_ms, end_ms, ...)` — must use a per-worker
  `requests.Session` mounted with an `HTTPAdapter` (see HTTP Client Shape);
  re-use the OKX `_RateLimiter` lock/clock pattern.
- `_repair_missing_minutes_for_day(coin, day_s, ...)`
- `_validate_day_minutes(...)`

## Source Index

Call `update_source_index_for_day(...)` for all written minutes. Use source
codes from `market_data_sources.py`:

- REST/API minutes use `SOURCE_CODE_API`.
- Bitget has no archive, so **no archive source code** is required.

No changes to `market_data_sources.py` are needed for Bitget (REST-only path
already covered by `SOURCE_CODE_API`).

## Task Worker Integration / API Integration / Frontend Integration

The per-component changes below are folded into the **Market Data Config / Auto
Downloads** section above, which now lists every concrete file edit. This section
captures only the cross-cutting concerns that span multiple files.

### Cross-cutting concerns

- **Manual backfill job can be started for Bitget** — handled by
  `BEST_1M_EXCHANGES["bitget"]` addition → enables `POST /best-1m/queue/bitget`
  and `GET /best-1m/info/bitget`.
- **Progress/status UI displays `bitget_best_1m` mode cleanly** — handled via
  `best1mJobMonitorMeta.bitget`, frontend `exchangeOptions`, status-key
  dispatch, and `task_worker` `progress["mode"]`.
- **Auto-download controls include Bitget** — handled by adding Bitget to
  `SETTINGS_EXCHANGES`, by the new `_bitget_latest_1m_loop`, and by
  `[bitget_data]` ini section.
- **`?v=N` cache-busting versions** for any changed frontend assets.

### Pre-edit gitnexus impact checks

Before modifying the symbols below, run `gitnexus_impact` / `api_impact` and
surface HIGH or CRITICAL warnings to the user before editing:

- `task_worker.py` `_run_<ex>_best_1m` dispatcher and the shared `jtype` switch
  (`task_worker.py:263-273` — a busy hub symbol with multiple dependents).
- `api/market_data.py` route handlers (`/best-1m/queue/{exchange}`,
  `/best-1m/info/{exchange}`, `/status-monitor/{exchange}`) — use
  `gitnexus_api_impact` for the affected `route` path.
- `api/heatmap.py` `_get_missing_lag_minutes` and `_build_coverage_heatmap` if
  bitmap filtering changes.

These impact runs are pre-edit checks for the author; they are not a blocker
for normal flow but must be reported in the commit message / PR description.

## Market Data Config / Auto Downloads

Bitget must be included wherever Binance/Bybit/OKX are included for automatic
1m downloads. This is the largest portion of the integration because PBGui
manages one independent background loop per exchange, not a single loop keyed by
a config dict. Each of the following touch points must be cloned for Bitget,
matching the OKX block line-for-line except for the `bitget` storage key.

### PBData.py — background latest-refresh loop

PBGui runs one `asyncio.Task` per exchange (`PBData.py:3824-3830`). Bitget
must be added as a fifth exchange, mirroring the OKX block exactly:

**Module import** (`PBData.py:27-30`):
```python
from bitget_best_1m import update_latest_bitget_1m_for_coin
```

**Per-loop state attrs** (next to the `self._okx_latest_1m_*` block at
`PBData.py:493-501`):
```python
# Bitget latest 1m auto-refresh settings
self._bitget_latest_1m_enabled = True
self._bitget_latest_1m_interval_seconds = 3600
self._bitget_latest_1m_coin_pause_seconds = 0.5
self._bitget_latest_1m_api_timeout_seconds = 30.0
self._bitget_latest_1m_min_lookback_days = 2
self._bitget_latest_1m_max_lookback_days = 7
self._bitget_latest_1m_task = None
```

**`_load_settings()` block** (after the OKX block ending around `PBData.py:770`):
read the same six keys from the `[bitget_data]` ini section, mirroring
`bnc_*`/`bbt_*`/OKX blocks. Default `interval_seconds=3600`,
`coin_pause_seconds=0.5`, `api_timeout_seconds=30`, `min_lookback_days=2`,
`max_lookback_days=7`.

**New method `_bitget_latest_1m_loop(self)`** — copy `_okx_latest_1m_loop`
(`PBData.py:1394`) and replace:
- exchange `"okx"` → `"bitget"`
- status key `"okx_latest_1m"` → `"bitget_latest_1m"`
- `update_latest_okx_1m_for_coin` → `update_latest_bitget_1m_for_coin`
- `bybit_get_newest_day`/`okx_get_newest_day` import →
  `from bitget_best_1m import get_newest_day as bitget_get_newest_day`
- `_okx_latest_1m_*` attr names → `_bitget_latest_1m_*`
- `_refresh_inventory_coin("okx", "1m", coin)` →
  `_refresh_inventory_coin("bitget", "1m", bitget_storage_coin_dir(coin))`
  after importing `get_storage_coin_dir as bitget_storage_coin_dir` from
  `bitget_best_1m`. Inventory refresh looks up exact on-disk directory names;
  raw coins like `PEPE` will not match `1000PEPE_USDT:USDT`.
- `asyncio.sleep(24)` startup offset → `asyncio.sleep(30)` to stagger past the
  OKX loop

**Task creation** in the shared pollers startup block (`PBData.py:3828-3830`):
```python
if not hasattr(self, "_bitget_latest_1m_task") or self._bitget_latest_1m_task is None or self._bitget_latest_1m_task.done():
    self._bitget_latest_1m_task = asyncio.create_task(self._bitget_latest_1m_loop())
```

**Run-now / stop flag files**: rely on `PBGDIR/data/logs/bitget_latest_1m_run_now.flag`
and `bitget_latest_1m_stop.flag` exactly as the OKX loop uses them. The flag
prefix comes from `api/market_data.py:_get_exchange_flag_prefix` (see below).

### api/market_data.py — settings, status-key, flag-prefix dispatch

Several hardcoded dispatch helpers must grow a Bitget branch:

- `SETTINGS_EXCHANGES` dict (`api/market_data.py:71`): add a `"bitget": {...}`
  entry copying the OKX block. Set `"ini_section": "bitget_data"`,
  `"label": "Bitget"`, and sensible defaults (interval 3600, pause 0.5,
  timeout 30, lookback 2..7).
- `BEST_1M_EXCHANGES` dict (`api/market_data.py:770`): add
  `"bitget": {"label": "Bitget", "job_type": "bitget_best_1m", "queue_exchange": "bitget", ...}`
  with a description mirroring Bybit (REST-only backfill, no archive).
- `COPY_DATA_EXCHANGES` dict (`api/market_data.py:813`): add
  `"bitget": {"label": "Bitget", "storage": "bitget"}`.
- `_get_exchange_status_key` (`api/market_data.py:680-692`): add branch
  `elif exchange == "bitget": return "bitget_latest_1m"`.
- `_get_exchange_flag_prefix` (`api/market_data.py:695-707`): add branch
  `elif exchange == "bitget": return "bitget_latest_1m"`.

> **Note**: These dispatch helpers return `""` for unknown exchanges today.
> Until Bitget branches are added, `/status-monitor/bitget` returns 404 even
> if the modules exist. The frontend would then show "Unknown exchange" —
> so the API branches must land in the same commit as the frontend dropdown.

### market_data.py — coin options + canonical enabled coins

`get_market_data_coin_options()` (`market_data.py:385`) currently calls
`coindata.filter_mapping(exchange=ex, ...)` with `quote_filter=["USDT"] if ex == "okx" else None`.
Bitget is a linear-perp exchange dominated by USDT pairs but with some USDC /
USD / SUSDT markets (`data/coindata/bitget/mapping.json` has 663 USDT, 48
USDC, 19 USD, 3 SUSDT, 2 SUSDC, 2 SUSD swap records). **Bitget must use
`quote_filter=["USDT"]`** like OKX to match PB7's USDT-perp focus — extend the
filter to `if ex in ("okx", "bitget")`.

This means in `market_data.py:391`:
```python
quote_filter=["USDT"] if ex in ("okx", "bitget") else None,
```
and the corresponding fallback `if ex == "okx" and quote != "USDT"` check at
`market_data.py:425` becomes `if ex in ("okx", "bitget") and quote != "USDT":`.

`_normalize_market_data_exchange` (`market_data.py:288`) and
`_canonical_enabled_coin` (`market_data.py:298`) are otherwise
exchange-neutral; they do not need a Bitget branch, but the storage coin-dir
normalization (next section) does.

### pbgui.ini / pbgui.ini.example

Add a new `[bitget_data]` section to `pbgui.ini.example` documenting the six
keys (`latest_1m_interval_seconds`, `latest_1m_coin_pause_seconds`,
`latest_1m_api_timeout_seconds`, `latest_1m_min_lookback_days`,
`latest_1m_max_lookback_days`). Add the explicit example block; do not rely on
implicit `_load_settings` defaults only.

### Inventory / cache refresh

`inventory_cache.refresh_coin("bitget", "1m", coin)` is exchange-neutral —
it scans `data/ohlcv/bitget/1m/<coin_dir>/` and updates the per-coin cache.
**No changes needed** in `inventory_cache.py`. The only requirement is that
`bitget_best_1m.py:_coin_dir(coin)` returns the correct directory name
(`BTC_USDT:USDT`), so `refresh_coin` finds the files.

### Auto-enable-new-coins + pruning

PBData's `_bitget_latest_1m_loop` (mirroring OKX) must call
`get_effective_enabled_coins("bitget", cfg=cfg)` and
`set_enabled_coins("bitget", coins)` for pruning. Both helpers are
exchange-neutral except for the coin-options filter change above, so no
further code changes are required beyond passing the right string.

### Per-day best-1m job dispatch (task_worker)

`task_worker.py:23-30` imports one `improve_best_<ex>_1m_for_coin` per
exchange. Add:
```python
from bitget_best_1m import improve_best_bitget_1m_for_coin
```

The `jtype` switch (`task_worker.py:263-270`) needs a new branch:
```python
elif jtype == "bitget_best_1m":
    _run_bitget_best_1m(job_path, payload)
```

Add `_run_bitget_best_1m(job_path, payload)` mirroring `_run_okx_best_1m`
(`task_worker.py:1610-1794`):
- `progress["mode"] = "bitget_best_1m"`
- `append_exchange_download_log("bitget", ...)` (not `"binanceusdm"` style —
  Bitget has no alias)
- import `improve_best_bitget_1m_for_coin` and
  `get_storage_coin_dir as bitget_storage_coin_dir`
- inventory refresh `_refresh_inventory_coin("bitget", "1m", bitget_storage_coin_dir(coin))`

### Queueing best-1m jobs

`POST /best-1m/queue/{exchange}` (`api/market_data.py:2557`) already reads
`meta = _best_1m_exchange_meta(exchange)`, so adding Bitget to the
`BEST_1M_EXCHANGES` dict (above) automatically enables `POST /best-1m/queue/bitget`.
No additional route code is required.

### Frontend latest-1m auto-refresh — `market_data_main.html`

Several hardcoded exchange lists exist:

| Section | Lines | Required change |
|---|---|---|
| Exchange dropdown | `2740-2745` | Add `<option value="bitget">Bitget</option>` |
| `exchangeOptions` array | `3236-3241` | Add `{ key: 'bitget', statusKey: 'bitget', label: 'Bitget' }` |
| `best1mJobMonitorMeta` | `3287-3291` | Add `bitget: { exchange: 'bitget', jobType: 'bitget_best_1m' }` |
| `best1mQueueMeta` | `3292-3297` | Add `bitget: { api: 'market-data', path: '/best-1m/queue/bitget' }` |
| Copy Data checkboxes | `3088-3091` | Add `<label class="settings-toggle"><input type="checkbox" data-copy-data-exchange="bitget" checked>Bitget</label>` |
| Status fetch (statusKey) | `3636` | Auto-handled if `exchangeOptions` has the entry — the meta lookup walks that array |

Increment `?v=N` cache-busting versions for `market_data_main.html` (and any
shared JS it mounts) in the FastAPI static mount.

### Status aggregation / heatmap legacy fallbacks

A few legacy mappings return hardcoded exchange buckets — none depend on a
chunk-by-chunk list, but some filters omit Bitget:

- `api/heatmap.py:283-296`: the 1m source-count heatmap only computes
  `day_counts` when `ex in ("hyperliquid", "binance", "bybit", "binanceusdm", "okx")`.
  **Add `"bitget"` to the tuple** — otherwise the heatmap will show "No data"
  for Bitget even when `sources.idx` files exist on disk.
- `api/heatmap.py:30-54` (`_STORAGE_EXCHANGE_MAP`, `_get_missing_lag_minutes`):
  Bitget needs a new `elif ex == "bitget": sec = load_ini("bitget_data", ...)` branch
  for the "missing minutes" lag computation; otherwise Bitget inherits the
  Hyperliquid default `pbdata` section's interval (1800 s), underestimating the
  actual freshness window.
- `api/jobs.py:176-182` and `frontend/jobs_monitor.html:607-615`: the legacy
  delete-by-exchange fallback maps `job.type` heuristic to an exchange. The
  production Bitget queue path always sets `exchange="bitget"` on the job
  payload (via `meta["queue_exchange"]`), so the legacy fallback is not used
  for new Bitget jobs. However, OKX is also missing from this fallback — this
  is a pre-existing minor bug. **Add Bitget anyway** to keep parity:
  - `api/jobs.py`: add `elif exchange_filter == "bitget" and (job_type.startswith("bitget_") or "bitget" in job_type): filtered_jobs.append(j)`
  - `frontend/jobs_monitor.html`: same JS branch

`balance_calc.py:49` already includes `"bitget"` in its `EXCHANGES` list —
no changes needed there.

## Tests

Add tests covering:

- Bitget symbol normalization (`BTC` -> `BTCUSDT`, `BTC/USDT:USDT`,
  `BTC_USDT:USDT`, `BTCUSDT`). Include sub-1 USD coins:
  `1000PEPE` -> `1000PEPE_USDT:USDT`.
- REST row parser maps `[ts,o,h,l,c,base_vol,quote_vol]` to `bv=base_vol`.
- NPZ dtype and `candles` key.
- `get_storage_coin_dir(coin)` and `_coin_dir(coin)` returns `BTC_USDT:USDT` /
  `1000PEPE_USDT:USDT` (not just `BTC`).
- `get_newest_day(coin)` and `get_oldest_day(coin)` return `YYYYMMDD` format,
  not `YYYY-MM-DD` (parity with Bybit/OKX; consumed by PBData via
  `datetime.strptime(newest_day, "%Y%m%d")`).
- Backward pagination chunk scheduler builds correct endTime cursors.
- Pagination overlap idempotence: when two adjacent cursors return the same
  minute, the last-write-wins dict update produces a single NPZ row, not a
  duplicate (regression for the GPT-flagged overlap concern).
- Missing minute detection.
- REST re-fill repairs known simulated gaps in a day.
- Inception day (731 minutes) is NOT flagged as `unrepaired_minutes` by the
  repair loop when `inception_ms` is above the day's missing minutes (regression
  for the boundary exception).
- Today (UTC) is NOT flagged as `unrepaired_minutes` by the repair loop.
- Retry behavior for `429` (HTTP status 429 AND JSON `code="429xx"` variants) —
  verifies silent retry up to `MAX_RETRIES`, then surfaces as a failed page.
- Retry behavior for transient network/5xx errors.
- Retry behavior: `code="40053"` (limit out of range) and `code="400171"`
  (bad granularity) are **not** retried — surfaced directly.
- Task-worker dispatch for `bitget_best_1m`.
- API job creation for Bitget (`BEST_1M_EXCHANGES["bitget"]`) — verifies
  `POST /best-1m/queue/bitget` returns `{"success": True,...}` with
  `job_type="bitget_best_1m"` and `exchange="bitget"`.
- Status-key dispatch: `_get_exchange_status_key("bitget") == "bitget_latest_1m"`
  and `_get_exchange_flag_prefix("bitget") == "bitget_latest_1m"`.
- Heatmap source-count tuple includes `"bitget"` (regression for the
  silent-"No data" bug).
- `get_market_data_coin_options("bitget")` returns only USDT-quote linear swaps
  (not USDC/USD/SUSDT/SUSD markets), parity with OKX USDT filtering.
- Per-worker `requests.Session` is reused across requests in the same thread
  (no `session.close()` between fetches, no per-request TLS handshake).
- Cross-exchange sanity: stored BTC prices match Binance within 0.5 % per
  minute for a sampled recent day (regression guard against timestamp shift).

Manual smoke tests:

- BTC short REST range: `2024-04-01..2024-04-03` (v2 probed at 1.17 s, 4400 rows).
- BTC 30-day range: `2024-04-01..2024-04-30` (v1 probed at 34.32 s, 43200 rows).
- BTC full history: `2019-07-10..T-1` (v2 probed at 518.84 s ≈ 8min 39s,
  3,664,091 rows, 2545 NPZ files, 0 interior incomplete days).
- BTC inception day: `2019-07-10` partial-day handling (731/1440).
- ETH short range.
- UI smoke: select Bitget in `market_data_main.html` dropdown, queue a small
  Best 1m job, watch status-monitor show it, and check the heatmap renders for
  the coin (verifies touch-points 5, 6, 8 in the index).

## Verification

Run targeted tests first, then full tests if safe:

```bash
/home/mani/software/venv_pbgui/bin/python -m pytest tests/
```

Do not run destructive tests or mutate real runtime data except through the
intended temporary/test paths.

## Changelog And Serial

For implementation (not this plan doc itself):

- Add/update `releases/unreleased.md`.
- Increment `api/serial.txt` by 1 because `api/market_data.py` and any
  API-startup-imported module changes count toward the runtime restart signal.
- Ask before committing or pushing.

## Constraints

- Do not deploy/copy/modify any bot/VPS host without explicit confirmation.
- Do not modify PB7 / PB6 legacy modules.
- Do not make large unsolicited design changes — this plan must be approved
  before implementation begins.

## Temporary Probe Artifacts

Temporary scripts/output used during planning (not part of the repo):

- `/tmp/opencode/bitget_btc_1m_npz_test.py` — original urllib probe (v1)
- `/tmp/opencode/bitget_btc_1m_npz_v2.py` — improved probe (v2):
  `requests.Session` per worker, connection pool, silent retry on 429.
  This is the architecture the production module should adopt.
- `/tmp/opencode/bitget_vs_binance_quality.py` — quality comparator (Bitget
  vs Binance/Bybit reference in `data/ohlcv/`).
- `/tmp/opencode/bitget_btc_1m_npz/data/ohlcv/bitget/1m/BTC_USDT:USDT` — v1 probe output
- `/tmp/opencode/bitget_btc_1m_npz_v2/data/ohlcv/bitget/1m/BTC_USDT:USDT` — v2 probe output
- `/tmp/opencode/bitget_full_run.log` — v1 full run log (738.95 s)
- `/tmp/opencode/bitget_v2_full.log` — v2 full run log (518.84 s)

These are not part of the repo and need not be copied into tests/fixtures unless
explicitly desired. The v2 script is the reference architecture for the
production `bitget_best_1m.py` module.
