# KuCoin Historical Market Data Assessment

## Decision

**Status: unsuitable for PBGui historical OHLCV integration. Do not implement
KuCoin as a historical `Build Best 1m` or automatic historical-download
exchange.**

Gate.io is likewise unsuitable: its public 1m REST history is limited to the
most recent 10,000 candles and no suitable official long-term source was
identified.

The KuCoin archive is technically reliable, but the representative quality
check demonstrated that native candle density is fundamentally unsuitable for
an exchange-wide historical dataset. Only 5 of 20 sampled markets reached 95%
native coverage, while established contracts such as BNB, NEAR, XMR, and LDO
required 20% to 37% replacement data. Lower-liquidity contracts required up to
86% replacement data. Native trade-archive checks confirmed that the missing
minutes predominantly represent real no-trade intervals, not recoverable
Kline corruption.

Filling such datasets from Binance or other exchanges would create a
synthetic cross-exchange price history and present it as KuCoin history. That
is not an acceptable basis for KuCoin backtests, irrespective of source-index
attribution. PBGui therefore excludes KuCoin and Gate.io from historical
OHLCV support unless their providers offer materially improved native history
in the future and a new quality assessment reverses this decision.

The remaining sections preserve the investigated design, measurements, and
rejected implementation approach as decision evidence. They are not an active
implementation plan.

## Investigated Goal

The investigated approach would have added KuCoin linear USDT perpetual
1-minute OHLCV data to PBGui with the same
external lifecycle as the existing exchange downloaders:

1. A user starts one initial `Build Best 1m` job.
2. The job imports the official KuCoin daily archive and completes the recent
   tail through KuCoin REST.
3. PBData subsequently refreshes enabled coins through KuCoin REST at the
   configured interval.
4. Missing native KuCoin minutes are filled from other exchanges and are
   identified as `other_exchange` in `1m_src`.

The scheduled latest updater must not repeat the full archive import. A long
outage beyond the configured REST lookback is repaired by rerunning the
history job for the missing range.

## Reference Implementations

- Use `okx_best_1m.py` as the main archive/REST and repair template.
- Use the PBData loops in `PBData.py` for settings reload, run-now, stop,
  status publishing, lookback calculation, and inventory refresh.
- Use `_fill_missing_from_exchange_perp_1m()` in
  `hyperliquid_best_1m.py` as the behavioral reference for external fallback
  candles and `SOURCE_CODE_OTHER` attribution.
- Preserve the NPZ and source-index contracts in `market_data_sources.py`.

## User Workflow

### Initial history build

Expose KuCoin in the generic Market Data `Build Best 1m` interface. The job
must support the existing date range, `refetch`, progress, cancellation,
retry, and resume behavior.

For each selected coin, the job must:

1. Resolve the PBGui coin, CCXT symbol, KuCoin REST contract ID, KuCoin archive
   symbol, contract size, and storage directory from the KuCoin mapping.
2. List the available official daily 1m archive objects for that coin.
3. Download only missing or explicitly refetched days in the requested range.
4. Validate and normalize each archive before atomically writing its day NPZ.
5. Fetch the recent overlap and current closed candles from KuCoin REST.
6. Fill remaining missing minutes from eligible other exchanges.
7. Update `1m_src` with the winning source for every written minute.
8. Refresh inventory and publish a final quality summary.

Existing valid day files are the resume checkpoint. A restarted job rescans
the target range and must not redownload completed days unless `refetch` is
set or archive metadata indicates that a recent object changed.

### Scheduled latest refresh

Add a dedicated `PBData._kucoin_latest_1m_loop()` equivalent to the Binance,
Bybit, OKX, and Bitget loops. It must process KuCoin coins enabled in the
shared Market Data configuration.

For every cycle and coin:

1. Determine the newest local KuCoin day.
2. Calculate the effective lookback as the larger of the configured minimum
   and local staleness, capped by the configured maximum.
3. Fetch closed 1m candles for that interval from KuCoin REST.
4. Merge by timestamp, with current KuCoin REST data replacing archive or
   fallback data for the same minute.
5. Fill still-missing closed minutes from other exchanges.
6. Atomically write changed day files and update their source codes.
7. Refresh inventory and publish per-coin status.
8. Observe the stop flag between coins and wait for the configured interval,
   interruptible through the run-now flag.

The current forming minute must never be stored. Prefer KuCoin server time for
the closed-minute cutoff, with local UTC as a logged fallback.

## Configuration

Add the same generic settings already exposed for other exchanges:

```ini
[kucoin_data]
latest_1m_interval_seconds = 3600
latest_1m_coin_pause_seconds = 0.5
latest_1m_api_timeout_seconds = 30
latest_1m_min_lookback_days = 2
latest_1m_max_lookback_days = 7
```

Saving settings must touch:

```text
data/logs/kucoin_latest_1m_run_now.flag
```

The API and frontend must expose the same run-now, stop, current coin, last
result, last run, next run, and error state as the existing exchange loops.

## Storage Contract

Use the canonical exchange key `kucoin`:

```text
data/ohlcv/kucoin/1m/<COIN>_USDT:USDT/YYYY-MM-DD.npz
data/ohlcv/kucoin/1m_src/<COIN>_USDT:USDT/sources.idx
```

The NPZ key remains `candles` with the standard dtype:

```python
np.dtype([
    ("ts", "i8"),
    ("o", "f4"),
    ("h", "f4"),
    ("l", "f4"),
    ("c", "f4"),
    ("bv", "f4"),
])
```

Protect the complete read-merge-write-index transaction with a cross-process
lock scoped to exchange, coin, and UTC day. Atomic replacement alone does not
prevent a history job and PBData latest refresh from losing each other's
updates.

## Symbol Resolution

Resolve markets from `data/coindata/kucoin/mapping.json`; do not construct a
REST or archive symbol without validating it against the mapping and archive
listing.

BTC demonstrates the required aliases:

| Purpose | BTC value |
| --- | --- |
| PBGui/CCXT | `BTC/USDT:USDT` |
| KuCoin REST | `XBTUSDTM` |
| KuCoin archive | `BTCUSDTM` |
| Storage | `BTC_USDT:USDT` |

Only active linear swaps with `quote == "USDT"` are offered. Resolve the
archive symbol by listing available archive prefixes and matching the REST ID
and mapped base aliases. Cache only a unique confirmed match; reject ambiguity.

## Official Archive

Root and per-symbol prefix:

```text
https://historical-data.kucoin.com/
data/futures/daily/klines/{ARCHIVE_SYMBOL}/1m/
```

Daily objects:

```text
{SYMBOL}-1m-YYYY-MM-DD.zip
{SYMBOL}-1m-YYYY-MM-DD.zip.CHECKSUM
```

There are no observed monthly futures kline archives. Build a paginated S3
manifest per symbol, parse exact dated ZIP keys, and use the manifest rather
than issuing a speculative request for every calendar day.

For every archive:

1. Validate the checksum object and ZIP MD5.
2. Read the complete ZIP member so CRC verification runs.
3. Reject unsafe paths, unexpected members, or an invalid CSV header.
4. Parse `time,open,high,low,close,volume` with `csv.reader`.
5. Validate UTC day membership, minute alignment, finite OHLC values, OHLC
   invariants, and non-negative volume.
6. Sort by timestamp because older observed archives are not ordered.
7. Deduplicate identical rows and reject conflicting duplicate timestamps.
8. Convert contract volume to base volume before float32 storage.

Do not reject a valid archive only because it contains fewer than 1,440 rows.
KuCoin omits intervals without ticks. Missing minutes continue into the repair
pipeline described below.

## Volume Normalization

Both archive and native REST volume represent contracts. Store PBGui base
volume as:

```text
base_volume = contract_volume * contract_size
```

For BTC, `contract_size` is `0.001`. The importer must compare the mapped
contract size with current KuCoin contract metadata. A mismatch blocks that
coin instead of silently mixing differently normalized history.

## KuCoin REST Tail

Use the native futures endpoint so pagination and rate-limit headers are under
PBGui control:

```text
GET https://api-futures.kucoin.com/api/v1/kline/query
symbol=<REST_ID>&granularity=1&from=<ms>&to=<ms>
```

Live validation on 2026-07-12 returned at most 200 candles per request. Use
200 as the production page size until a tested endpoint change justifies a
larger value.

For a half-open interval `[start, end)`:

1. Align boundaries to UTC minutes.
2. Request windows of at most 200 minutes.
3. Sort, validate, and deduplicate each response.
4. Discard rows outside the requested window and the forming minute.
5. Advance by the requested window, not the last returned candle, because
   no-tick intervals may be absent.

Use one shared process-local limiter for history jobs and PBData. Start with a
five-request-per-second ceiling and low concurrency. Observe
`gw-ratelimit-limit`, `gw-ratelimit-remaining`, and `gw-ratelimit-reset`, keep
a reserve for other PBGui activity, and honor server reset timing after 429.

Retry only network failures, timeouts, HTTP 408/429, and transient 5xx errors.
Invalid symbols, schema failures, and persistent data conflicts fail
deterministically after bounded attempts.

## Cross-Exchange Gap Fill

Missing native KuCoin minutes must not remain unfilled when equivalent linear
USDT perpetual data is available from another supported exchange.

Apply this deterministic source priority:

1. KuCoin REST
2. KuCoin verified archive
3. Binance USDT perpetual
4. Bybit linear USDT perpetual
5. OKX USDT swap
6. Bitget linear USDT perpetual
7. Missing

Only exact UTC minute matches are eligible. Resolve every fallback through its
`data/coindata/<exchange>/mapping.json`, require a compatible linear USDT
perpetual, and preserve the fallback candle's already normalized base volume.

For each gap-repair pass:

1. Compute missing closed minute indices after merging native KuCoin data.
2. Read matching local PBGui NPZ days from the fallback exchanges first.
3. If local data is absent and the provider supports the requested history,
   fetch only the required containing windows through that provider's normal
   downloader/rate limiter.
4. Fill only missing KuCoin minutes; never replace native KuCoin candles with
   another exchange.
5. Continue through the priority list only for minutes still missing.
6. Report counts by fallback exchange and unresolved minute count.

When a later KuCoin REST or archive refresh supplies a formerly missing
minute, the native KuCoin candle replaces the fallback candle and its source
code changes from `SOURCE_CODE_OTHER` to `SOURCE_CODE_API`.

### Source-index semantics

For KuCoin:

| Stored code | Meaning |
| ---: | --- |
| `0` | missing |
| `1` | native KuCoin archive or REST |
| `3` | candle copied from another exchange |

`market_data_sources._source_label_for_code()` currently displays code `3` as
`api` for every non-Hyperliquid exchange because older indexes used that code
for official archives. Extend the interpretation so code `3` is displayed as
`other_exchange` for KuCoin while preserving existing labels for exchanges
whose old indexes rely on the compatibility mapping. Add regression tests for
both paths.

The first implementation does not need a source-index format migration: the
existing two-bit format already distinguishes native and fallback minutes.

## Recent Archive Reconciliation

The initial history job must use KuCoin REST as authoritative for a recent
overlap, defaulting to the same two-day minimum lookback used by scheduled
refreshes. REST rows replace archive rows for matching timestamps. Archive-only
rows remain native KuCoin rows.

Relist and revalidate only the newest archive dates during an active history
job. PBData does not poll the archive after the initial build; it advances the
dataset through REST.

## Long Outages

PBData must cap automatic recovery at `latest_1m_max_lookback_days`. If the
local dataset is older than that cap, update the recent tail, expose the older
gap in status/inventory, and require a bounded rerun of `Build Best 1m`.

Do not let a routine latest cycle silently become a multi-year archive job.

## Implementation Surface

Create:

- `kucoin_best_1m.py`
- `tests/market_data/test_kucoin_best_1m.py`

Integrate:

- `market_data.py`: eligible KuCoin coin discovery.
- `market_data_sources.py`: KuCoin code-3 `other_exchange` interpretation.
- `PBData.py`: settings, latest loop, run-now/stop, startup, and shutdown.
- `task_worker.py`: `kucoin_best_1m` dispatch, progress, cancellation, resume,
  result logging, and inventory refresh.
- `api/market_data.py`: build metadata, settings, status, and flags.
- `api/heatmap.py`: KuCoin refresh lag.
- `frontend/market_data_main.html`: generic KuCoin exchange registration.
- `frontend/jobs_monitor.html`: KuCoin job-to-exchange mapping if required.
- `pbgui.ini.example`: `[kucoin_data]` example.
- English and German Market Data/PBData help.
- `releases/unreleased.md` and `api/serial.txt`.

Public downloader entry points:

- `improve_best_kucoin_1m_for_coin(...)`
- `update_latest_kucoin_1m_for_coin(...)`
- `get_newest_day(coin)`
- `get_oldest_day(coin)`
- `get_storage_coin_dir(coin)`

## Test Matrix

Archive and normalization tests must cover manifest pagination, BTC/XBT alias
resolution, checksum mismatch, corrupt ZIP, unsafe member names, unsorted CSV,
duplicate conflicts, partial listing/delisting days, missing no-tick minutes,
contract-size conversion, NPZ dtype, atomic replacement, and resume/refetch.

REST tests must cover the 200-candle boundary, inclusive API timestamps,
no-tick empty windows, overlap deduplication, forming-minute exclusion,
rate-limit headers, retry classification, and invalid symbols.

Gap-fill tests must cover source priority, local NPZ reuse, provider fallback,
native-data non-overwrite, code `3` attribution, UI label
`other_exchange`, unresolved gaps, and later native replacement changing the
source code to `1`.

Lifecycle tests must cover job dispatch/progress/cancel/retry/resume, PBData
lookback calculation, settings hot reload, run-now, stop, error isolation,
inventory refresh, concurrent history/latest writes, and deterministic
shutdown.

## Rejected Approach Acceptance Criteria

1. A BTC history job imports all available verified KuCoin archives in its
   requested range and resumes without redownloading valid days.
2. Archive and REST produce identical normalized candles in a tested overlap.
3. Missing KuCoin minutes are filled according to the documented exchange
   priority whenever a fallback minute exists.
4. Native and fallback minutes are respectively stored as source codes `1`
   and `3`, and the KuCoin UI reports code `3` as `other_exchange`.
5. A later native KuCoin candle replaces an existing fallback candle and
   changes its source attribution to native.
6. PBData periodically appends closed REST candles at the configured interval
   without re-running the archive import.
7. Rate limits, cancellation, atomic writes, and restart recovery remain safe
   under concurrent history and latest activity.
8. Inventory, heatmap, status, job monitor, and English/German help expose the
   same functional coverage as existing exchanges.
9. Offline tests pass without reading or writing production runtime data.

## Pre-Implementation Validation

Before production implementation, run an isolated BTC archive benchmark under
`/tmp/opencode`:

1. Benchmark unique representative daily ZIPs at increasing concurrency.
2. Stop increasing when errors, throttling, or marginal throughput gains make
   the next level unsafe.
3. Download the full BTC archive with the selected safe concurrency.
4. Validate checksums, ZIP CRC, schema, timestamps, duplicates, and gaps.
5. Convert every day to the intended PBGui NPZ schema under the temporary
   root only.
6. Record elapsed time, bytes, candles, missing minutes, failures, and the
   recommended production concurrency.

The benchmark must never write to `data/ohlcv/` or any other PBGui runtime
directory.

## BTC Validation Results

An isolated full BTC validation was completed on 2026-07-12. All artifacts
remain outside PBGui runtime data under:

```text
/tmp/opencode/kucoin_btc_archive_test/
```

### Archive concurrency

Disjoint representative archive samples were tested at concurrency levels 1,
2, 4, 8, 12, 16, 24, 32, 48, and 64. All requests completed successfully.
Throughput stopped improving after 48 workers; 64 workers were slower and had
higher latency.

The production starting value for static KuCoin archive downloads is **32
parallel workers**:

- 32 workers delivered 12.66 archives/second in the representative sample.
- This was about 87% of the measured 48-worker peak.
- Median and P95 latency remained materially lower than at 48 or 64 workers.
- The full run completed faster after connection and provider caches warmed,
  but the conservative configured value remains 32.

This limit applies only to the static archive host. KuCoin REST uses the
separate low-rate shared limiter described above.

Benchmark reports:

```text
/tmp/opencode/kucoin_btc_concurrency.json
/tmp/opencode/kucoin_btc_concurrency_high.json
```

### Full KuCoin BTC archive import

The full listed BTC archive was downloaded, checksum-verified, parsed,
normalized to PBGui NPZ, and indexed with 32 workers:

| Metric | Result |
| --- | ---: |
| Archive range | 2023-01-01 through 2026-07-11 |
| Listed archives | 1,288 |
| Successful archives | 1,288 |
| Failed archives | 0 |
| Downloaded ZIP bytes | 32,159,554 |
| Native KuCoin candles | 1,846,955 |
| Missing native minutes | 7,765 |
| Days containing gaps | 643 |
| Duplicate timestamps | 0 |
| Unsorted source archives | 1,178 |
| End-to-end elapsed time | 46.99 seconds |

The generated temporary dataset occupies about 65 MiB including original
ZIPs, daily compressed NPZ files, source index, and reports. A REST overlap
comparison for 2025-01-01 matched archive OHLC and contract-size-normalized
base volume within the expected float32 storage tolerance.

Full report:

```text
/tmp/opencode/kucoin_btc_archive_test/report.json
```

### Binance gap-fill validation

All 7,765 missing KuCoin BTC minutes were then requested from checksum-verified
official Binance USDT perpetual daily archives. The temporary KuCoin NPZ files
were updated only at missing timestamps and those source-index minutes were
changed to code `3`.

| Metric | Result |
| --- | ---: |
| KuCoin days requiring fallback | 643 |
| Missing minutes requested | 7,765 |
| Minutes filled from Binance | 7,765 |
| Unresolved minutes | 0 |
| Errors | 0 |
| Fallback concurrency | 16 |
| Elapsed time | 58.64 seconds |

Final temporary source counts:

| Code | Meaning | Minutes |
| ---: | --- | ---: |
| `0` | missing | 0 |
| `1` | native KuCoin | 1,846,955 |
| `2` | l2Book | 0 |
| `3` | Binance fallback | 7,765 |

The source counts total 1,854,720 minutes, exactly 1,288 complete UTC days.

Gap-fill report:

```text
/tmp/opencode/kucoin_btc_archive_test/cross_exchange_fill_report.json
```

## Representative Multi-Coin Quality Check

An additional full-history quality scan was completed on 2026-07-12 after the
BTC validation. Twenty active archived KuCoin crypto USDT perpetuals were
selected across current 24-hour turnover ranks 1 through 530. Expected
coverage starts at each contract's actual `firstOpenDate`, so pre-listing
minutes on the first archive day are not counted as gaps.

The scan downloaded and parsed 16,340 daily Kline archives, representing
232.78 MiB of compressed data, in 359.27 seconds with 32 workers. All archives
were readable and no archive request failed.

| Rank | Coin | Archive days | Native coverage | Fallback needed | Longest gap |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | ETH | 1,288 | 99.040% | 0.960% | 190 min |
| 2 | BTC | 1,288 | 99.581% | 0.419% | 189 min |
| 3 | SOL | 1,288 | 99.347% | 0.653% | 189 min |
| 6 | EVAA | 282 | 96.971% | 3.029% | 72 min |
| 11 | ADA | 1,288 | 93.788% | 6.212% | 191 min |
| 21 | XMR | 1,288 | 63.098% | 36.902% | 198 min |
| 36 | NEAR | 1,288 | 79.937% | 20.063% | 192 min |
| 51 | BNB | 1,288 | 79.916% | 20.084% | 192 min |
| 76 | LDO | 1,288 | 72.004% | 27.996% | 213 min |
| 101 | FF | 286 | 96.843% | 3.157% | 64 min |
| 126 | ETHFI | 846 | 67.805% | 32.195% | 162 min |
| 151 | 10000REKT | 6 | 84.642% | 15.358% | 99 min |
| 176 | ALPINE | 431 | 42.245% | 57.755% | 757 min |
| 201 | ZEST | 47 | 81.268% | 18.732% | 35 min |
| 251 | MOVR | 919 | 22.155% | 77.845% | 785 min |
| 301 | BEL | 1,171 | 25.943% | 74.057% | 841 min |
| 351 | PROMPT | 453 | 63.491% | 36.509% | 257 min |
| 401 | ILV | 949 | 21.589% | 78.411% | 439 min |
| 451 | TSTBSC | 516 | 21.149% | 78.851% | 632 min |
| 530 | USDG | 131 | 14.447% | 85.553% | 2,849 min |

The worst Kline day for every sampled coin was compared with the official
native KuCoin trade archive for the same contract and date. All 20 trade
archives were available. None contained a trade in a minute missing from its
Kline archive. This demonstrates that the dominant gaps are genuine KuCoin
no-trade intervals rather than failed Kline aggregation. Consequently, a high
fallback percentage materially changes the dataset from KuCoin execution
history into another exchange's price history.

Current 24-hour turnover rank is not a sufficient quality proxy. Established
markets such as XMR, NEAR, BNB, and LDO still required 20% to 37% fallback over
their full archive history. Eligibility must therefore use measured native
minute coverage, not market name or current volume alone.

Full machine-readable report:

```text
/tmp/opencode/kucoin_quality_check/report.json
```

### Quality policy

Classify each completed native KuCoin history import before cross-exchange
repair:

| Native coverage | Status | Behavior |
| ---: | --- | --- |
| `>= 95%` | eligible | Allow automatic cross-exchange gap fill and normal PBData latest refresh. |
| `>= 90%` and `< 95%` | warning | Keep data and show the measured fallback requirement; do not auto-enable the coin as a recommended KuCoin backtest source. |
| `< 90%` | unsuitable | Do not present the completed composite as a meaningful KuCoin backtest dataset and do not silently fill most of it from other exchanges. |

Also require at least 90 completed archive days before assigning a stable
quality classification. Newer contracts remain `provisional`; they may be
downloaded explicitly but are not auto-enabled based on a short initial
sample.

Store and expose these metrics per coin:

- expected minutes since `firstOpenDate`
- native KuCoin minutes and percentage
- fallback minutes and percentage
- affected days
- longest consecutive native gap
- coverage classification and calculation timestamp

The selected 95% automatic threshold admitted ETH, BTC, SOL, EVAA, and FF in
this sample. ADA received warning status at 93.788%. The remaining 14 sampled
markets were unsuitable. The threshold is a data-quality boundary, not a
claim that fallback candles reproduce KuCoin order execution.
