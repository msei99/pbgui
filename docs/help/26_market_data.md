# Market Data

This page manages PBGui market-data workflows for Hyperliquid, including l2Book archive downloads, TradFi symbol mapping, and Build best 1m OHLCV jobs.

## Page Layout

Expanders are shown in this order:
1. Market Data status
2. Build best 1m OHLCV
3. TradFi Symbol Mappings
4. Download l2Book from AWS

## Market Data Status

Use this section to monitor latest fetch loops, inventory, and background job health.

Highlights:
- Read-only inventory for PBGui and PB7 cache data
- Source-code based coverage views
- Job progress with day/month context for stock-perp builds

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

This starts background `hl_best_1m` jobs for eligible XYZ symbols.

Eligibility in coin selector:
- Symbol must have mapping status `ok`
- Must have Tiingo mapping (`tiingo_ticker` or `tiingo_fx_ticker`)

Controls:
- Build best 1m
- Start date (optional)
- End date (optional)
- Refetch TradFi data from scratch (stock-perps)

### Data strategy

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

### Progress and waits

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
