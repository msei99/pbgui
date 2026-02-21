# Market Data

This page manages **PBGui-managed market data downloads** per exchange, with focus on **Hyperliquid l2Book** (order book snapshots) and derived **1-minute candles**.

## Overview

The Market Data page provides:
- Download of **Hyperliquid l2Book** raw data from AWS S3 Archive (Requester Pays)
- **"Build best possible 1m archive"** - automatic creation of optimal 1m candles from multiple sources
- Display of existing raw files (Inventory)
- **PB7 cache status view** (read-only) for `pb7/caches/ohlcv/<exchange>`
- Download log per exchange

## PB7 Cache Status (Inventory Tab)

In **Already have**, there is an additional tab: **PB7 cache**.

It shows a read-only inventory of PB7 cache files from:

`pb7/caches/ohlcv/<exchange>/<timeframe>/<coin>/YYYY-MM-DD.npy`

Displayed fields:
- `timeframe` (e.g. `1m`, `1h`)
- `coin`
- `n_files`
- `size`
- `oldest_day` / `newest_day`
- `n_days`

Top metrics summarize total timeframes, coins, files, and disk usage.

Notes:
- This tab is informational only (no delete actions).
- If PB7 path is missing/misconfigured, the tab may be empty.

---

## Hyperliquid: l2Book Archive (AWS S3)

### What is l2Book?

l2Book files are hourly **Order Book Snapshots** (Limit Order Book Level 2) from Hyperliquid as `.lz4` compressed files. These can be converted to high-precision 1-minute candles.

### Download Process

1. **Enter AWS Credentials:**
   - `AWS profile name`: Profile name (e.g. `pbgui-hyperliquid`)
   - `aws_access_key_id` and `aws_secret_access_key`: IAM credentials with S3 access
   - `AWS region`: `us-east-2` (default for Hyperliquid archive)

2. **Select Coins:**
   - Multiselect: Individual coins or `All` for all enabled coins
   - **No coin is pre-selected** - explicit selection required

3. **Set Date Range:**
   - **Start date**: First date to download (default: oldest available date in archive)
   - **End date**: Last date to download (default: newest available date in archive)
   - Tooltips show archive boundaries

4. **Click "Auto download l2Book":**
   - Job is queued in background
   - Worker starts automatically if not active
   - **Auto-trigger Build OHLCV:** After l2Book download completes, "Build best 1m" is automatically enqueued for each coin that received new data
     - Only coins with actual downloads trigger a build job
     - Saves manual step of running "Build best 1m" separately

### Cost Optimization

**Important:** The Hyperliquid S3 Archive is **Requester Pays** - you pay for:
- S3 GET requests (~$0.0004 per 1,000 requests)
- Data transfer (~$0.09 per GB)

**Skipped Files = No Cost:**
```
planned:24 downloaded:0 skipped:24 failed:0 (13.3 MB)
```
- `skipped:24` = local files already exist, **no S3 request**
- `downloaded:0` = no new downloads, **no transfer cost**
- `failed:0` = no failed requests

**The download checks locally first** if files exist before contacting S3!

### Connection Pooling

The download uses a **single boto3 session** for all parallel downloads:
- Shares TCP connections between threads
- Reduces SSL handshakes
- Faster downloads through connection reuse

### Storage Location

Downloaded l2Book files:
```
pbgui/data/ohlcv/hyperliquid/l2Book/<COIN_CCXT>/<YYYYMMDD>-<H>.lz4
```

Example:
```
pbgui/data/ohlcv/hyperliquid/l2Book/KBONK_USDC:USDC/20231120-01.lz4
pbgui/data/ohlcv/hyperliquid/l2Book/BTC_USDC:USDC/20250210-15.lz4
```

**COIN_CCXT Format:**
- `BTC_USDC:USDC` - Standard format
- `KBONK_USDC:USDC` - K-prefix for special coins (BONK, PEPE, FLOKI, SHIB, LUNC, DOGS, NEIRO)

---

## Build Best Possible 1m Archive (Auto)

### What Does This Function Do?

Creates an **optimal 1-minute archive** for each coin through intelligent combination of multiple data sources:

### Data Source Priority

For each missing minute, sources are checked in this order:

1. **API 1m** (if already downloaded)
   - Uses existing API downloads from `1m_api/`
   - Only for empty slots (SOURCE_CODE_MISSING)

2. **l2Book → 1m Conversion** (highest quality)
   - Converts **local** l2Book files to 1m candles
   - Uses Order Book mid-price for OHLCV
   - **No comparison with S3** - only local files

3. **Binance USDT-Perp Gap Fill** (fallback 1)
   - Downloads missing minutes from Binance USDT perpetuals
   - **Smart Gap Smoothing:**
     - `open` of first gap minute = `close` of previous l2Book candle
     - `close` of last gap minute = `open` of next l2Book candle
   - Smooths transitions between different data sources

4. **Bybit USDT-Perp Gap Fill** (fallback 2)
   - Downloads remaining gaps from Bybit USDT perpetuals
   - Important for tokens like HYPE that listed on Binance later
   - Same smart smoothing logic as Binance

### Workflow

```
For each day:
  1. Check if day already complete (1440 minutes)
     → Skip processing (Optimization 1)
     ↓
  2. Insert API 1m (if available)
     ↓
  3. Convert local l2Book → 1m (optimized parser)
     ↓
  4. Fill remaining gaps with Binance (only if gaps exist)
     ↓
  5. Fill still open gaps with Bybit (only if gaps exist)
     ↓
  6. Update source codes
```

### Performance Optimizations

**1. Skip Complete Days (Optimization 1)**
- Days with 1440 minutes (complete) are skipped entirely
- Re-runs become ~100x faster (0.01s vs 1.6s per complete day)
- Only processes days with missing data

**2. Conditional API Calls (Optimization 2)**
- Binance/Bybit are only called when gaps actually exist
- No unnecessary API calls for complete l2Book days
- Reduces network overhead and rate limit pressure

**3. Fast l2Book Processing (~47% faster)**
- Optimized JSON parser (orjson instead of stdlib json)
- Direct bytes parsing (skips UTF-8 decode step)
- Efficient float arithmetic for mid-price calculation
- **Result:** ~1.6s per day (down from ~2.4s)
- **Impact:** 100 days processed in ~2.7 minutes (was ~4 minutes)

### Source Code Tracking

Each minute receives a code for traceability:
- `SOURCE_CODE_L2BOOK` = calculated from local l2Book (best quality)
- `SOURCE_CODE_API` = from Hyperliquid API
- `SOURCE_CODE_OTHER` = from Binance/Bybit (other exchange)
- `SOURCE_CODE_MISSING` = empty / no data

### Output Folders

**Best 1m files:**
```
pbgui/data/ohlcv/hyperliquid/1m/<COIN>/YYYY-MM-DD.npz
```

**API 1m raw downloads:**
```
pbgui/data/ohlcv/hyperliquid/1m_api/<COIN>/YYYY-MM-DD.npz
```

**Source index:**
```
pbgui/data/ohlcv/hyperliquid/_source_index/<COIN>/<YYYYMMDD>.npy
```

### Typical Log Output

**Summary after build:**
```
[hl_best_1m] BTC improve: days=180 l2book_added=12450 binance_filled=3580 bybit_filled=0
[hl_best_1m] HYPE improve: days=90 l2book_added=85000 binance_filled=2500 bybit_filled=1850
```

- `days=180` - days checked
- `l2book_added=12450` - minutes converted from l2Book
- `binance_filled=3580` - minutes loaded from Binance
- `bybit_filled=1850` - minutes loaded from Bybit (e.g., for HYPE token)

**Detailed timing logs (optional):**

To enable detailed per-day performance metrics, set environment variable:
```bash
export PBGUI_TIMING_LOGS=1
```

Then logs will include timing breakdowns:
```
[TIMING] SOL 20241001 total=1.608s read=0.000s src_idx=0.000s api=0.000s 
         l2book=1.604s l2write=0.004s binance=0.000s bybit=0.000s 
         existing=1440 l2added=1440

[TIMING] SOL 20241006 total=2.729s read=0.000s src_idx=0.000s api=0.000s 
         l2book=1.897s l2write=0.006s binance=0.820s bybit=0.005s 
         existing=1438 l2added=1438
```

**Timing breakdown:**
- `total` - complete processing time for the day
- `read` - reading existing 1m npz file
- `src_idx` - loading source index
- `api` - processing API 1m data
- `l2book` - converting l2Book to 1m (~1.6s per day typical)
- `l2write` - writing l2Book-derived 1m to disk
- `binance/bybit` - fetching gap fills (only if needed)
- `existing` - minutes already present before processing
- `l2added` - minutes added from l2Book conversion

---

## Recommended Workflow

### 1. Initial Setup

```
1. Enter AWS credentials in Market Data
2. Enable coins (via Enable/Disable toggle)
3. Download l2Book for desired date range
   → "Build best 1m" is automatically triggered for downloaded coins
4. Wait for both jobs to complete (l2Book download + Build OHLCV)
```

### 2. Regular Updates

```
1. l2Book download (downloads only new/missing hours)
   → skipped files = free, no re-downloads
   → "Build best 1m" auto-triggered for coins with new data
2. Done! No manual "Build best 1m" needed
```

### 3. Fill Gaps

```
1. Check inventory - identify missing time ranges
2. Set date range precisely (Start/End date)
3. Download l2Book for gap
   → "Build best 1m" auto-triggered
4. Complete! Gap filled automatically
```

### 4. Manual Build (Optional)

```
Run "Build best 1m" manually only if:
- Need to rebuild without new l2Book data
- Want to re-process with different settings
- Testing or troubleshooting
```

---

## AWS Credentials Management

### Saving Profiles

PBGui stores AWS credentials as profiles:

**Credentials:** `~/.aws/credentials`
```ini
[pbgui-hyperliquid]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

**Region:** `~/.aws/config`
```ini
[profile pbgui-hyperliquid]
region = us-east-2
```

### IAM Permissions

Minimum required S3 permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject"],
    "Resource": "arn:aws:s3:::hyperliquid-archive/*"
  }]
}
```

---

## Troubleshooting

### High AWS Costs

**Avoid by:**
- Only select needed coins and date ranges
- Don't download same data multiple times (check `skipped` in log)
- Use larger date ranges in one job instead of many small jobs

### l2Book present but 1m not generated

**Check:**
1. Do l2Book files exist in correct directory?
   ```
   ls -lh pbgui/data/ohlcv/hyperliquid/l2Book/BTC_USDC:USDC/
   ```

2. Wait for auto-triggered "Build best 1m" job to complete
   - Check Task Queue for active `hl_best_1m` jobs
   - Or run "Build best 1m" manually

3. Check log:
   ```
   [hl_best_1m] BTC improve: days=X l2book_added=0 ...
   ```
   If `l2book_added=0` → l2Book files not recognized

### Build Best 1m is slow

**Expected performance:**
- Complete days: ~0.01s per day (skipped via Optimization 1)
- l2Book processing: ~1.6s per day
- With Binance/Bybit gaps: +0.8-1.0s per day

**If significantly slower:**
1. Enable timing logs: `export PBGUI_TIMING_LOGS=1`
2. Check which operation is slow
3. l2Book > 3s per day → check disk I/O (SSD recommended)
4. Binance/Bybit > 2s per day → check network/API issues

### Build Best 1m not auto-triggered after l2Book download

**Check:**
1. Did l2Book download actually download new files?
   - Look for `downloaded:N` where N > 0 in log
   - `skipped:24` = no new files = no trigger
2. Check Task Queue for `hl_best_1m` jobs
3. Manually trigger "Build best 1m" if needed

### Source index cleanup after deletion

**Automatic cleanup:**
- Quick delete: Removes entire 1m_src directory for coin
- Multiselect delete: Batch removes for all selected coins
- Date-based delete: Incremental update (zeros deleted days)
- Clear dataset: Iterates all coins and cleans indexes

No manual cleanup of source indexes needed!

**Check:**
1. Do l2Book files exist in correct directory?
   ```
   ls -lh pbgui/data/ohlcv/hyperliquid/l2Book/BTC_USDC:USDC/
   ```

2. Run "Build best 1m" again

3. Check log:
   ```
   [hl_best_1m] BTC improve: days=X l2book_added=0 ...
   ```
   If `l2book_added=0` → l2Book files not recognized

---

## Technical Details

### l2Book Format

- **Compression:** LZ4 (fast, moderate compression)
- **Granularity:** Hourly (H = 0-23, single-digit for 0-9)
- **File size:** ~700-800 KB per hour (compressed)
- **Content:** JSON lines with L2 order book snapshots
- **Bucket:** `hyperliquid-archive` (us-east-2)
- **S3 Path:** `market_data/YYYYMMDD/H/l2Book/<coin>.lz4`

### l2Book Conversion Performance

**Processing pipeline:**
1. LZ4 decompression (~15% of time)
2. JSON parsing with orjson (~75% of time)
3. Mid-price calculation from bid/ask (~10% of time)

**Optimizations:**
- **orjson parser:** 37% faster than stdlib json
- **Direct bytes parsing:** Skip UTF-8 decode (12% faster)
- **Float arithmetic:** Faster than Decimal for intermediate calculations
- **Combined:** ~47% improvement (2.4s → 1.6s per day)

**Typical rates:**
- ~22,000 L2 snapshots per hour file
- ~110,000 mid prices per second processing rate
- ~1.6 seconds per day (24 hour files)

### Special Coins (k-Prefix)

Hyperliquid uses K-prefix for meme coins with many zeros:
- `BONK` → `kBONKUSDC` (symbol) → `KBONK_USDC:USDC` (directory)
- `PEPE` → `kPEPEUSDC` → `KPEPE_USDC:USDC`

**List:** BONK, FLOKI, LUNC, PEPE, SHIB, DOGS, NEIRO

### Coin Normalization

The pipeline converts between different formats:

```
UI Input           → Symbol Lookup     → Directory         → S3 Key
─────────────────────────────────────────────────────────────────────
BONK (normalized)  → kBONKUSDC         → KBONK_USDC:USDC  → kBONK
BTC (normalized)   → BTCUSDC           → BTC_USDC:USDC    → BTC
```

### Parallelization

- **l2Book Download:** 8 parallel workers (default)
- **Shared S3 Client:** Connection pooling across all threads
- **Retry Logic:** Automatic for transient errors
- **Build OHLCV:** Sequential per day, but days processed independently

### Source Index Management

**Binary format:** 360 bytes per day (1 byte per minute + padding)
- Each byte encodes data source for that minute
- Fast incremental updates on deletion (no full rebuild)
- Memory-mapped for efficient access

**Deletion strategies:**
- **Complete coin:** Remove entire `1m_src/<COIN>/` directory
- **Date range:** Zero out bytes for deleted days (fast incremental)
- **Clear dataset:** Iterate coins, clean each index

---

## Notes

- This page does **not** delete old files - manual cleanup required
- l2Book conversion happens **on-demand** during "Build best 1m"
- PB7 CandlestickManager uses the `1m/` files as source
- You can select PBGui OHLCV data as the data source in Backtest and Optimize
- Files are stored as `.npz` to reduce disk usage (roughly half the space)
- After l2Book -> 1m conversion, you can delete l2Book files to save disk space; as long as `1m/` files remain, l2Book is no longer required. Future downloads still fetch only new l2Book data because `1m_src` tracks what is already available.
- **Dependencies:** Requires `orjson>=3.9.0` (installed automatically with PBGui)

---

## Configuration

### Auto-Refresh Settings (PBData Background Service)

PBData automatically refreshes the latest 1m candles from Hyperliquid API in the background.

**Configure via GUI:**
- Market Data page → "Settings (Latest 1m Auto-Refresh)" expander
- Change values and click "Save Settings"
- Changes are applied automatically in the next refresh cycle (no restart needed)

**Or configure via `pbgui.ini`:**

```ini
[pbdata]
# Auto-refresh interval for latest 1m API fetches (default: 120 seconds / 2 minutes)
latest_1m_interval_seconds = 300  # Example: 5 minutes for many symbols

# Pause between individual coins to avoid rate limits (default: 0.5 seconds)
latest_1m_coin_pause_seconds = 1.0  # Example: 1 second pause per coin

# API request timeout per coin (default: 30 seconds)
latest_1m_api_timeout_seconds = 45.0  # Example: 45 seconds for slow connections

# Lookback window for API fetches (default: 2-4 days)
latest_1m_min_lookback_days = 2
latest_1m_max_lookback_days = 4
```

**Why increase the interval?**
- **Default:** 120 seconds (2 minutes) works for ~20-30 coins
- **Many symbols:** Increase to 300-600 seconds (5-10 minutes) when fetching all Hyperliquid symbols
- **Rate limits:** Hyperliquid API has throttling - larger intervals prevent issues

**Why increase the coin pause?**
- **Default:** 0.5 seconds between coins prevents burst requests
- **Rate limit issues:** Increase to 1-2 seconds if seeing 429 errors
- **Many symbols:** Longer pauses = more total time per cycle, adjust `latest_1m_interval_seconds` accordingly

**Why increase API timeout?**
- **Default:** 30 seconds per coin API request
- **Slow connections:** Increase to 45-60 seconds if requests timeout frequently
- **Many candles:** Larger lookback windows need longer timeouts

**How it works:**
1. PBData cycles through all enabled coins every N seconds
2. Each coin: fetch lookback window (2-4 days), overwrite existing minutes
3. Status visible in "Market Data status" expander (shows next_run_in_s per coin)
4. Settings are reloaded automatically each cycle - changes take effect immediately
```
