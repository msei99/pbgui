# Market Data

This page manages PBGui market-data workflows for Hyperliquid, Binance USDM, and Bybit, including l2Book archive downloads, TradFi symbol mapping, 1m auto-refresh loops, and Build best 1m OHLCV jobs.

## Recommended Workflow — Best Practice

This is the fastest, most storage-efficient way to have all coins up to date so backtests start immediately.

### Step 1 — Enable all coins for Auto-Refresh

1. Open **Settings (Binance USDM Latest 1m Auto-Refresh)** → click **Select all** → **Save**
2. Open **Settings (Latest 1m Auto-Refresh) — Hyperliquid** → click **Select all** → **Save**
3. Switch exchange dropdown to **Bybit** → open **Settings (Bybit Latest 1m Auto-Refresh)** → click **Select all** → **Save**

This registers all coins for the rolling update loop. The loop will keep the last few days current automatically — no further manual action needed after the initial backfill.

### Step 2 — Run "Build best 1m all" for the initial backfill

Go to **Build best 1m OHLCV** and click **Build best 1m all** (or select all coins and submit).

This queues one background job per exchange that downloads the complete history from inception:

| Exchange | Download method | Expected duration (first run) |
|---|---|---|
| **Binance** | Parallel monthly + daily ZIPs (data.binance.vision) + CCXT fill | ~2–4 hours (~550 coins) |
| **Bybit** | CCXT (async) | ~3 hours (~550 coins) |
| **Hyperliquid** (crypto) | l2Book archive + 1m\_api conversion | depends on l2Book archive size |
| **Hyperliquid** (XYZ stock-perps) | Tiingo IEX/FX 1m | depends on number of mapped symbols + Tiingo quota |

**Benchmarks from actual runs:**
- Binance LINK (6+ years, 2 239 days, 74 monthly ZIPs): **41 s** with parallel ZIP download
- Binance all ~550 coins (parallel ZIPs): **estimated 2–4 h** (extrapolated: avg. coin ~3 years ≈ 24 monthly ZIPs → ~20 s/coin)
- Bybit all 548 coins (CCXT, observed): **~3 h** (BTC alone = 102 min, short coins add proportionally little)

Both jobs run in the background. You can close the browser and come back. Use the **Running** panel to watch progress.

### Step 3 — Verify the last completed job

After the job finishes, open the **Done** job in the job panel and click **🔍** (raw JSON). Check:
- `status: done` (not `failed`)
- `last_result.days_checked` — matches expected coverage
- `last_result.minutes_written` > 0
- Any `notes` entries (e.g. `monthly_download_failed=...` means the daily-ZIP fallback was used for that month — normal if the most recent month ZIP is not yet published)

### Step 4 — Auto-Refresh keeps data current

After the initial backfill, the daily update is automatic:

- Binance: latest **2–7 days** are refreshed via CCXT every 3 600 s (1 h) per cycle
- Bybit: latest **2–7 days** are refreshed via CCXT every 3 600 s (1 h) per cycle
- Hyperliquid: latest **2–4 days** are refreshed via API every 1 800 s (30 min) per cycle

For immediate refresh hit **⏩ Run now** in the respective **Market Data Status** panel.

### Why this approach

- **Minimal disk usage** — data is stored as compressed `.npz` files (one per day per coin); `.npz` is ~35% smaller than PB7's uncompressed `.npy` cache — e.g. BTC/USDT Binance: **61 MB** (pbgui `.npz`, Sep 2019 – today) vs **89 MB** (PB7 `.npy` cache, Dec 2019 – today)
- **Backtests start instantly** — no on-demand fetching needed; the local files are pre-built and ready
- **Incremental** — subsequent "Build best 1m all" runs skip already-complete days (pre-scan), only new data is downloaded
- **No duplicate storage** — one `.npz` per day per coin replaces any previously partial version

---

## Page Layout

Expanders are shown in this order:
1. Settings (Latest 1m Auto-Refresh) — Hyperliquid
2. Settings (Binance USDM Latest 1m Auto-Refresh)
3. Market Data status (Hyperliquid)
4. Market Data status (Binance USDM)
5. Build best 1m OHLCV
6. TradFi Symbol Mappings
7. Download l2Book from AWS

## Market Data Page

The `Market Data` page now runs directly on the FastAPI implementation, and the sidebar exposes the settings area through three dedicated subsections:

The sidebar itself is now navigation-only: it contains the main page sections plus the contextual `Settings` actions, without separate overview or status summary info boxes.

- `Coin Refresh` — exchange refresh settings and the enabled-coins workflow
- `AWS / l2Book` — Hyperliquid archive download settings
- `TradFi / Tiingo` — Tiingo credentials and TradFi mapping controls

The shared `Guide` button on that page opens this `Market Data` topic directly inside the page overlay, so the current Market Data view stays visible while you read.

The sidebar no longer shows a separate `Actions` section. Instead it exposes direct shortcuts that stay inside the page:

- `OHLCV Data` stays inside FastAPI too: when that panel is active, the sidebar reveals dataset buttons for the selected exchange instead of in-panel tabs.
- `Build Best 1m` opens a dedicated FastAPI panel for the current exchange.
- `Download l2Books` opens the embedded Hyperliquid data-actions panel directly when `Hyperliquid` is selected.

`Build Best 1m` and `Download l2Books` now also use the same active button highlight as the other Market Data sidebar entries, so the currently open shortcut section is visible directly in the sidebar.

Inside that FastAPI `Best 1m` panel, Hyperliquid reuses the full download/build actions component in a focused way: `Best 1m` shows only the build content, and `Download l2Books` shows only the download content. The extra outer header card, nested window chrome, and the expander header itself are removed there so only the actual form content remains visible.

Hyperliquid `Best 1m` now also matches the newer FastAPI editing patterns more closely: the build range uses the same editor-style popup calendar as the Backtest/Optimize editors, and the coin chooser is rendered as a multi-column enabled-coins grid with `Filter enabled coin list`, `Select visible`, and `Clear all` instead of the old compact dropdown. The visible coin rows are directly clickable now and also support mouse-drag selection so larger ranges can be marked or cleared without checkbox clicking. Fast drag moves now interpolate the rows between cursor updates as well, so quick paint-style selection no longer skips coins.

Hyperliquid `Download l2Books` now uses that same coin-grid pattern too instead of the old compact dropdown. You can filter the enabled coin list, click visible rows directly, bulk-select the current filtered slice, clear the explicit selection, or drag across the visible grid to paint larger download ranges quickly. `XYZ-*` / TradFi symbols are excluded there because Hyperliquid l2Book archive downloads only apply to native coins. Leaving the selection empty still queues all remaining downloadable coins.

The focused Hyperliquid panel now also re-fits its embedded height when you switch between `Best 1m` and `Download l2Books`, so the shorter download view no longer keeps the empty tail and extra scrollbar from the previously taller build view.

The embedded Hyperliquid view also avoids a second internal page scrollbar now, so scrolling stays on the main Market Data page instead of splitting between the page and the focused panel.

For the archive-backed exchanges, the coin chooser now uses a settings-style enabled-coins grid directly in the FastAPI panel: `Filter enabled coin list` narrows the grid, `Select visible` adds the current filtered slice, `Clear all` resets the explicit selection, and you can drag across the visible coin rows with the mouse to add or remove larger ranges quickly. Fast drag movement now fills the intermediate rows too, so quickly painting through the grid no longer loses coins between mouse events. Leaving the selection empty still queues all enabled coins, while any explicit selection limits the queued Best 1m job to exactly those coins.

That FastAPI `Best 1m` view now also starts directly with the build fields for Binance and Bybit. The redundant intro header text and the extra top `Refresh` button were removed.

For Binance and Bybit, the FastAPI `Best 1m` build panel also shows the filtered Job Monitor directly below the full build form again, so you can watch queued, running, done, and failed `Best 1m` jobs for the selected exchange without leaving the panel.

That build area is flatter now as well: the coin/build section no longer sits inside an extra rounded card frame, and the embedded Job Monitor drops its standalone page chrome so the whole view reads as one continuous Market Data panel.

That embedded Job Monitor now also grows with its own content height, so you no longer get a second scrollbar inside the monitor area while the outer Market Data page is already scrollable.

The embedded monitor URL now carries the current PBGui serial as a cache-buster, so frontend updates also refresh the iframe itself and new monitor actions such as `View` show up immediately without staying on an older cached copy.

Hyperliquid uses its own inline data-actions page instead of that shared iframe, and that inline Job Monitor now also includes the same `View` action for active, done, and failed jobs so the details modal is consistent across Market Data and `System -> Services`. Pending rows in both monitor variants now also expose `Run`, which requests one extra manual same-type parallel slot so one selected pending job can start alongside the already running job of that type. Active rows stay in stable queue/start order now as well, so live progress updates no longer reshuffle two running jobs back and forth. `View` and `Log` dialogs in both variants are capped to the visible browser viewport too, and they now follow both the browser scroll position and clipping parent panels such as the scrollable `Build Best 1m` container, so their close button stays inside the actually visible monitor area instead of opening above it.

Its action dialogs are styled in-page now as well: cancel, delete, retry, requeue, and bulk-delete confirmations no longer fall back to browser-native popup windows.

The FastAPI `OHLCV Data` panel now follows the same parity goal. The selected exchange gets dataset buttons directly in the sidebar: `1m` and `PB7 cache` are always available, while Hyperliquid also shows `1m_api` and `l2Book`. The main panel then keeps the same workflow as Streamlit: summary metrics, a filterable inventory table, deletion tools for writable datasets, a coverage heatmap, a minute heatmap when available, and an optional OHLCV detail chart. `PB7 cache` remains read-only.

That FastAPI OHLCV detail chart now uses the same lazy zoom strategy as the Streamlit version. The initial iframe only ships coarse layers, so long histories open reliably again, and wheel zoom pulls finer candles on demand instead of trying to embed the full `15m` / `5m` / `1m` pyramid up front.

The iframe template itself is now served as real HTML/JS again, so the chart no longer stalls on a blank `Loading chart...` panel because of escaped quote characters inside the embedded script.

In Hyperliquid `OHLCV Data` → `l2Book`, the toolbar next to `Select All` / `Deselect` now also exposes a default-off toggle to include enabled non-XYZ coins that still have no l2Book files at all. That makes it possible to spot coins with completely missing l2Book coverage directly in the inventory table instead of only seeing coins that already have at least one archived hour.

The `OHLCV Data` sidebar stays button-only now. `Delete older than` was replaced by `Delete by Date`; clicking it opens a small dialog with the cutoff date picker and the delete preview instead of embedding that extra input block permanently in the sidebar.

That dialog now also mirrors the clearer Backtest editor date control pattern more closely: the cutoff field has a visible calendar button, and the current delete scope shows the selected coin names in a small scrollable list so multi-coin deletes stay explicit before you confirm them.

The final delete confirmation now also stays inside the PBGui styling: instead of the browser-native popup, delete actions open a centered confirmation window with the current scope and selected coins when applicable.

When you select one or more coins in `OHLCV Data`, the sidebar exposes the queue action that matches the current dataset view. In `1m`, `1m_api`, and `PB7 cache`, that remains `Build best 1m` for the selected coins on the current exchange. In Hyperliquid `l2Book`, the sidebar instead exposes an l2Book download queue action for those selected coins, so the inventory view no longer offers the unrelated Best 1m job there. The inventory sidebar itself is now button-only: queue/delete confirmations and errors no longer stay in persistent sidebar callouts, but go through the normal toast/notification path or the existing confirmation dialogs instead. The visible coin labels in this inventory UI now use the short coin name only, including the table, sidebar action buttons, and the heatmap/OHLCV captions.

In `PB7 cache`, the toolbar above the table now also includes a small timeframe quick filter next to `Select All` and `Deselect`. Use it to switch between `all`, `1m`, and `1h` rows before selecting coins, which avoids the short-name duplicates that appear when the same coin exists in both cached timeframes.

In Hyperliquid inventory views, the type filter now also supports `xyz only`, `xyz mapped`, and `xyz not mapped`. The table shows a `mapping` column for Hyperliquid rows, so you can immediately see the effective TradFi mapping status for each visible XYZ instrument, including statuses such as `mapped`, `no provider`, or `pending`. Active XYZ instruments are no longer shown as `delisted` just because an old entry in `tradfi_symbol_map.json` was not refreshed yet; when the live Hyperliquid mapping still lists the symbol, PBGui now resolves an active non-delisted status instead.

The inventory table now also uses the same mouse-selection behavior as the FastAPI Backtest/Optimize tables: clicking toggles a single row, dragging across rows adds or removes a contiguous range, and `Select All` only selects the rows that are currently visible after filtering.

The inventory table headers are sortable as well. Clicking a column header toggles between ascending and descending order for the currently visible rows in that dataset view.

## Settings (Latest 1m Auto-Refresh) — Hyperliquid

Controls the automatic 1m candle refresh loop for Hyperliquid symbols.

- **Enabled coins** — multiselect from all known Hyperliquid symbols
- **Select all / Clear all** — quickly enable or disable all coins
- **Cycle interval (s)** — how often all enabled coins are refreshed (default: 1800s)
- **Pause between coins (s)** — delay between coins to avoid rate limits (default: 0.5s)
- **API timeout per coin (s)** — per-coin request timeout (default: 30s)
- **Min / Max lookback days** — window for the latest fetch (default: 2 / 4 days)
- Changes are saved to `pbgui.ini` and applied in the next cycle — no restart needed.

Hyperliquid latest-1m catch-up requests can now reserve the full configured 4-day `candle_snapshot` budget correctly. A previous burst-cap mismatch in the local rate limiter could force repeated `budget_timeout` results even when the API request itself was valid.

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

Short toast messages from the Market Data Status panel and the Gap Heatmap are also written to PBGui's global notification log now, so you can reopen them later from the top-right notification bell instead of relying on the brief in-page popup only.

### Control Buttons

- **⏩ Run now** — skips the remaining wait and triggers the next refresh cycle immediately
- **⏹ Cancel queued refresh** — appears instead of Run now when a refresh is already queued; cancels it before the cycle starts
- **⏹ Stop current run** — appears during an active cycle; sends a stop signal so PBData aborts after the current coin finishes

### Progress Bar

While a cycle is running, a progress bar shows `coins done / total` and the current coin being processed.

### Status Table

Shows per-coin result of the last completed cycle:
- Only coins from the current enabled-coins set are shown; the FastAPI monitor filters stale rows immediately, and the next PBData cycle also drops them from the stored status.
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
- The overview and minute heatmaps on the FastAPI page keep Plotly wheel zoom disabled, and their Plotly modebar appears only on hover. Normal page scrolling therefore does not accidentally zoom those heatmaps, but the plot tools are still available when needed
- The chart uses lazy zoom: fully zoomed out it shows coarse candles (typically `1d`) and automatically recalculates finer timeframes when zooming in — no manual timeframe selection needed
- On the FastAPI page, those finer candles are fetched on demand inside the iframe, which keeps very long histories responsive instead of front-loading the full fine-resolution payload
- Those FastAPI lazy loads now use much smaller timeframe-specific windows and only fetch the exact fine layer that is currently needed, which keeps zoom interactions noticeably snappier
- The FastAPI chart opens in pan mode and keeps its Y axes movable, so after zooming you can drag the visible candles up or down instead of being forced to keep the auto-fitted vertical position
- The FastAPI chart now also keeps your chosen Plotly interaction mode across rerenders and snaps pans/zooms back to the real candle span, so it no longer unexpectedly flips tools or drifts into an empty chart window
- Stale FastAPI zoom requests are now aborted as soon as you move again, and same-timeframe pans avoid extra re-layout work unless the visible span really changed, which keeps the chart more responsive during rapid inspection
- At the data edges, FastAPI now keeps your current zoom span and shifts it against the nearest valid boundary instead of bouncing back to the full range, which makes dragging near the ends feel much more natural
- FastAPI now also merges newly fetched fine windows into the already loaded client-side layer instead of replacing it, so candles you just inspected do not vanish again as soon as you pan a bit further
- FastAPI now also treats zoom and pan clamps differently: zooms clip to the actual selected overlap with loaded data, while pans keep their span at the edges. That makes rectangle zoom behave much closer to the area you actually selected
- When you zoom back out but still stay inside the same fine timeframe, FastAPI now reloads that same timeframe if the cached client-side window no longer covers most of the visible range. That avoids cases where the chart still showed `1m` but large parts of the selected window were empty
- FastAPI now also tracks already loaded fine-timeframe windows as separate client-side coverage intervals instead of collapsing them into one `first candle .. last candle` block. That means zoom-out checks can see real uncovered holes between previously loaded windows and fetch them instead of leaving blank regions inside the visible chart area
- When FastAPI reloads the same fine timeframe, it now redraws the actual Plotly traces instead of updating only the layout. That ensures newly fetched candles become visible immediately instead of leaving the chart badge at `1m` while the missing section still looks empty
- FastAPI now also checks the actual number of loaded candles inside the current same-timeframe view. If a `1m` / `5m` / `15m` window is effectively empty despite the current timeframe badge, it triggers a same-timeframe reload instead of trusting coverage heuristics alone
- FastAPI now also normalizes Plotly relayout ranges that come without an explicit timezone before it clamps or rerenders the chart. Deep `1m` zoom-outs therefore stay on the intended time window instead of jumping back by the browser's local timezone offset
- FastAPI now also normalizes Plotly wheel/relayout timestamps with higher fractional precision before reusing them. That avoids rare deep `1m` wheel zoom-outs where the visible range could collapse into an empty sliver even though candles existed in the intended window
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

The inline mapping editor stays hidden by default and only opens when you explicitly click `Edit`.

Row 1 (selected-symbol workflow):
- Search ticker
- Edit
- Test Resolve
- Fetch start date
- Refresh spec

Row 2 (global workflow):
- Auto-Map
- Fetch all start dates
- Refresh metadata
- Refresh prices
- View specs

The action result box below the buttons can be closed again, and Auto-Map results expose expandable categories such as `Not found` and `Skipped` so you can inspect which symbols were affected.

The Tiingo widget above this section is a PBGui-local tracker, not the authoritative Tiingo dashboard usage view. PBGui labels those cards as local counters now, and it also shows a warning when Tiingo has returned a live `server_429` backoff. That means you can see the current retry wait directly even when the local `Hour` / `Day` / `Month Bandwidth` counters have not reached zero yet.

Auto-Map summary counts now follow the same non-delisted mapping rows that are visible in the table, so old delisted leftovers from the raw JSON file are no longer mixed into the result totals.

Auto-Map now also reconciles those visible rows against the current Hyperliquid XYZ activity before deciding to skip them. That means an active row with a stale raw `delisted` flag in `tradfi_symbol_map.json` is processed as active again, and descriptive stock texts such as `LLY tracks ... Eli Lilly and Company` now pass the Tiingo name check instead of landing in `Skipped`.

Pending rows keep a single `auto-map: not found` note marker, so repeated Auto-Map runs no longer spam the Note column with duplicate fragments.

TradFi type handling now follows the live XYZ specification cache more closely: the spec parser reads the dedicated Description and Underlying columns, and Auto-Map decides between direct lookup, FX mapping, and `no_provider` from the derived instrument type instead of relying only on a static symbol list.

`Search ticker` now opens in the floating PBGui utility window itself: you can edit the Tiingo query there, run the search, inspect the visible result list with the current Tiingo price when available, compare it with the current Hyperliquid price for the selected XYZ symbol, and apply a match directly from the same window. If Tiingo has no quote for a hit, the price is shown as unavailable instead of a misleading `0.0000`. Search hits with Tiingo exchange suffixes such as `BNO:BAT` are also matched against the underlying Tiingo quote ticker automatically, so they can still show the correct price.

### Specs Popup

`View specs` opens a popup with:
- Source/fetched timestamp/row count
- Link to original XYZ specification page
- A floating window that can be moved, resized, and closed like the other PBGui utility windows
- Large table view using most of the window height
- Clickable links:
  - Pyth Link
  - HL Link

Pyth links now preserve the encoded symbol separator required by `pythdata.app`, so symbols like `AMZN/USD` open through `%2F` instead of landing on a 404 page.

### Notes

- `Fetch start date` is equity-only (daily metadata endpoint).
- FX symbols do not use a dedicated start-date metadata fetch button.
- Auto-Map and metadata/price refresh require a configured Tiingo API key.

## Download l2Book from AWS

Downloads Hyperliquid l2Book archive files (Requester Pays).

On the FastAPI page, the Hyperliquid download panel now uses the same enabled-coins grid selector as `Best 1m`: `Filter enabled coin list` narrows the visible slice, `Select visible` adds the filtered rows in one step, `Clear all` resets the explicit selection, and you can click or drag across visible rows to build a download set quickly. `XYZ-*` / TradFi symbols are filtered out here because there is no Hyperliquid l2Book archive download for them. If you leave the selection empty, PBGui still queues all remaining downloadable Hyperliquid coins.

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

On the FastAPI page, Binance USDM and Bybit use a settings-style enabled-coins grid directly in the `Best 1m` build panel. You can narrow the list with `Filter enabled coin list`, click single rows, drag across visible rows to add or remove larger ranges quickly, or bulk-add the current filtered slice via `Select visible`. If you leave the explicit selection empty, PBGui queues all enabled coins for the current exchange.

On Hyperliquid, the focused `Best 1m` build panel now uses the same `Filter enabled coin list` + multi-column grid pattern for coin selection and the shared popup calendar style for `Start date` / `End date`, replacing the older single-row dropdown and browser-native date fields. The visible coin rows can be clicked directly or selected in larger ranges by dragging the mouse across the grid.

### Job Types

**`hl_best_1m`** — Hyperliquid XYZ stock-perps:
- Eligibility: mapping status `ok` + Tiingo ticker present
- Controls: Build best 1m, Start date, End date, Refetch TradFi from scratch

**`binance_best_1m`** — Binance USDM full historical backfill:
- Downloads complete inception-to-today 1m OHLCV from official Binance archives (data.binance.vision) — monthly + daily ZIPs — with CCXT gap-fill
- Coin selection from all enabled Binance coins
- Controls: Start date, End date, Refetch
- Storage: `data/ohlcv/binanceusdm/1m/<COIN>/YYYY-MM-DD.npz` (compressed NumPy archive; PB7 cache uses uncompressed `.npy` — ~35% larger for the same data)

### Job Management

The job panel shows three sections:
- **Pending** — jobs queued for execution
- **Running** — currently executing job with live progress
- **Failed / Done** — completed jobs

Actions:
- **Run** — marks one pending job for manual priority and allows one additional same-type job to start in parallel with the already running one
- **View** — opens the full job details (summary, payload, progress, last result)
- **Cancel** — requests cooperative cancellation for a running job from the embedded monitor; the worker stops at the next safe checkpoint
- **Retry** — requeues a failed job to Pending
- **Delete** — removes individual job
- **Delete selected / Delete all** — bulk delete from Failed or Done list

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
