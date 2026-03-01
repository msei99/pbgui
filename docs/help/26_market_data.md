# Market Data

This page manages PBGui market-data workflows for Hyperliquid and Binance USDM, including l2Book archive downloads, TradFi symbol mapping, Binance 1m auto-refresh, and Build best 1m OHLCV jobs.

## Page Layout

Expanders are shown in this order:
1. Settings (Latest 1m Auto-Refresh) — Hyperliquid
2. Settings (Binance USDM Latest 1m Auto-Refresh)
3. Market Data status (Hyperliquid)
4. Market Data status (Binance USDM)
5. Build best 1m OHLCV
6. TradFi Symbol Mappings
7. Download l2Book from AWS

## Settings (Latest 1m Auto-Refresh) — Hyperliquid

Controls the automatic 1m candle refresh loop for Hyperliquid symbols.

- **Enabled coins** — multiselect from all known Hyperliquid symbols
- **Select all / Clear all** — quickly enable or disable all coins
- **Cycle interval (s)** — how often all enabled coins are refreshed (default: 1800s)
- **Pause between coins (s)** — delay between coins to avoid rate limits (default: 0.5s)
- **API timeout per coin (s)** — per-coin request timeout (default: 30s)
- **Min / Max lookback days** — window for the latest fetch (default: 2 / 4 days)
- Changes are saved to `pbgui.ini` and applied in the next cycle — no restart needed.

## Settings (Binance USDM Latest 1m Auto-Refresh)

Controls the automatic 1m candle refresh loop for Binance USDM perpetuals.

- **Enabled coins** — multiselect from all known Binance USDM coins
- **Select all / Clear all** — quickly enable or disable all coins
- **Cycle interval (s)** — how often all enabled coins are refreshed (default: 3600s)
- **Pause between coins (s)** — delay between coins (default: 0.5s)
- **API timeout per coin (s)** — per-coin request timeout (default: 30s)
- **Min / Max lookback days** — window for the latest fetch (default: 2 / 7 days)
- Changes are saved to `pbgui.ini` and applied in the next cycle — no restart needed.

## Market Data Status

Use this section to monitor latest fetch loops, inventory, and background job health.

The status expander auto-refreshes every 5 seconds while open.

### Control Buttons

- **⏩ Run now** — skips the remaining wait and triggers the next refresh cycle immediately
- **⏹ Cancel queued refresh** — appears instead of Run now when a refresh is already queued; cancels it before the cycle starts
- **⏹ Stop current run** — appears during an active cycle; sends a stop signal so PBData aborts after the current coin finishes

### Progress Bar

While a cycle is running, a progress bar shows `coins done / total` and the current coin being processed.

### Status Table

Shows per-coin result of the last completed cycle:
- `last_fetch` — timestamp of last attempt
- `result` — `ok`, `error`, or `skipped`
- `lookback_days` — days fetched
- `minutes_written` — candles written in that run
- `note` — `no_local_data` means no local data existed yet; max lookback was used automatically
- `next_run_in_s` — estimated seconds until next cycle

### Restart Behavior

When PBData restarts, it reads the last run timestamp and waits the remaining interval — it does not immediately re-fetch. If PBData crashed mid-cycle, the run resumes from the last completed coin.

---
- Read-only inventory for PBGui and PB7 cache data
- Source-code based coverage views
- Job progress with day/month context for stock-perp builds
- In stock-perp minute view, overlay highlights for `market holiday` and `expected out-of-session gap` can be toggled off to inspect raw missing gaps directly
- Minute view includes an optional `OHLCV chart` expander with interactive Plotly candlesticks and volume bars for fast visual validation
- The chart uses lazy zoom: fully zoomed out it shows coarse candles (typically `1d`) and automatically recalculates finer timeframes when zooming in — no manual timeframe selection needed
- The coin name is shown as a label in the top-left corner of the chart
- For equity stock-perps, historical stock split dates are shown as vertical dashed orange lines with annotations (e.g. "Split 20:1"); OHLCV data is automatically adjusted for splits
- Split factor data is stored per exchange in `data/coindata/hyperliquid/split_factors.json` (fetched from Tiingo Daily API)

## TradFi Symbol Mappings

This section is the control center for XYZ stock-perp symbol routing.

### Table

The mapping table is built from:
- Hyperliquid mapping data (`mapping.json`)
- Manual/enriched entries (`tradfi_symbol_map.json`)

Displayed columns include:
- Symbol (Hyperliquid link)
- HL Price / Tiingo Price
- Description / Type / Status
- Start Date / Fetch Start
- Pyth link
- Verification and notes

Table filters:
- Filter by status
- Filter by symbol (matches XYZ symbol and Tiingo symbol/ticker)
- Filter by type (canonical type, e.g. `equity_us`, `fx`)

Start-date semantics:
- Start Date: provider metadata (`tiingo_start_date`)
- Fetch Start: effective earliest fetch date
  - IEX equity uses `max(Start Date, 2016-12-12)`
  - Empty when Start Date is unknown

### Action Buttons

Buttons are arranged in two aligned rows.

Row 1 (selected-symbol workflow):
- Search ticker
- Edit
- Test Resolve
- Fetch start date
- Spec

Row 2 (global workflow):
- Auto-Map
- Fetch all start dates
- Refresh metadata
- Refresh prices
- View specs

### Specs Popup

`View specs` opens a popup with:
- Source/fetched timestamp/row count
- Link to original XYZ specification page
- Large table view using most of the dialog height
- Clickable links:
  - Pyth Link
  - HL Link

### Notes

- `Fetch start date` is equity-only (daily metadata endpoint).
- FX symbols do not use a dedicated start-date metadata fetch button.
- Auto-Map and metadata/price refresh require a configured Tiingo API key.

## Download l2Book from AWS

Downloads Hyperliquid l2Book archive files (Requester Pays).

Workflow:
1. Configure AWS profile and region
2. Select coins and date range
3. Run auto download job

UI behavior:
- The download job queue is shown directly below the download controls
- `Last download job` is a collapsible summary panel
- The summary includes status, coins, range, counts (downloaded/skipped/failed), size stats, progress %, and duration

Cost behavior:
- Existing local files are skipped first
- Skipped files do not trigger S3 transfer/download work

Storage path:
- `data/ohlcv/hyperliquid/l2Book/<COIN>/<YYYYMMDD>-<H>.lz4`

## Build best 1m OHLCV

This starts background build jobs for eligible symbols.

### Job Types

**`hl_best_1m`** — Hyperliquid XYZ stock-perps:
- Eligibility: mapping status `ok` + Tiingo ticker present
- Controls: Build best 1m, Start date, End date, Refetch TradFi from scratch

**`binance_best_1m`** — Binance USDM full historical backfill:
- Downloads complete inception-to-today 1m OHLCV from official Binance archives (data.binance.vision) — monthly + daily ZIPs — with CCXT gap-fill
- Coin selection from all enabled Binance coins
- Controls: Start date, End date, Refetch
- Storage: `data/ohlcv/binanceusdm/1m/<COIN>/YYYY-MM-DD.npz` (same format as PB7 cache)

### Job Management

The job panel shows three sections:
- **Pending** — jobs queued for execution
- **Running** — currently executing job with live progress
- **Failed / Done** — completed jobs

Actions:
- **Retry** — requeues a failed job to Pending
- **Delete** — removes individual job
- **Delete selected / Delete all** — bulk delete from Failed or Done list
- **Raw JSON** (🔍 button) — shows full job file content for debugging

### Progress Display

While running, the panel shows:
- Stage: `starting`, `running`, `done`
- Current coin
- Chunk done / total
- Minutes written
- Duration
- For Binance: pages fetched, days covered
- For HL TradFi: month YYYY-MM day X/Y, Tiingo quota usage, 429 wait states

### Data Strategy (hl_best_1m)

Build best 1m runs newest → oldest in the selected date window.

For crypto symbols (non-XYZ):
- Uses local `1m_api` and local `l2Book` conversion first
- Fills remaining gaps from perp exchange fallback data
- `l2Book` is only used in this crypto path (not for XYZ stock-perps)

For FX-mapped stock-perps (`tiingo_fx_ticker`):
- Uses Tiingo FX 1m in weekly chunks (to reduce request count)
- Uses existing `other_exchange` history as anchor when not refetching
  - Start cursor = oldest existing `other_exchange` day minus 1 day
- `Refetch` starts from the selected/end day and rebuilds backwards in the allowed range
- Weekend session boundary uses observed feed behavior:
  - Friday close = 17:00 New York local time (DST-aware in UTC)
  - Sunday reopen ≈ 22:00 UTC (fixed)
- Known reduced FX holiday sessions:
  - `12-24` and `12-31`: early close around 22:00 UTC
  - `12-25` and `01-01`: late reopen around 23:00 UTC

For equity-mapped stock-perps (`tiingo_ticker`):
- Uses Tiingo IEX 1m
- Uses existing `other_exchange` history as anchor when not refetching
  - Start cursor = oldest existing `other_exchange` day minus 1 day
- Lower bound remains `max(tiingo_start_date, 2016-12-12)`
- Raw-first write behavior: any minute bars returned by Tiingo are written (no extra market-hours clipping in the write path)

Write safety rules:
- TradFi writes (`other_exchange`) only fill missing minutes or minutes already marked as `other_exchange`
- Existing `api` / `l2Book_mid` minutes are not overwritten by TradFi

Date controls:
- `Start date` limits the oldest day to process
- `End date` limits the newest day to process (default = today)

### Progress and Waits (hl_best_1m)

Job panel can show:
- `month YYYY-MM day X/Y`
- Tiingo month request usage
- Quota/429 wait states with wait seconds and reason

## Tiingo Settings (in page settings)

This page provides Tiingo controls:
- `tiingo_api_key`
- Test Tiingo button
- Runtime quota indicators (hour/day/month bandwidth)
- External links for API key signup and usage dashboard

## Troubleshooting

If a build job appears briefly and disappears:
1. Check the latest failed job in `data/ohlcv/_tasks/failed`
2. Confirm worker is running the latest code (restart worker if needed)
3. Verify Tiingo key and symbol mapping status
4. Use `Test Resolve` for the selected symbol

If Build coin list is empty:
- Ensure symbols are mapped and status is `ok`
- Ensure Tiingo ticker or FX ticker exists in mapping
