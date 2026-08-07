# Market Data

This page manages PBGui market-data workflows for Hyperliquid, Binance USDM, Bybit, OKX, and Bitget, including l2Book archive downloads, TradFi symbol mapping, 1m auto-refresh loops, and Build best 1m OHLCV jobs.

## Recommended Workflow — Best Practice

This is the fastest, most storage-efficient way to have all coins up to date so backtests start immediately.

### Step 1 — Enable all coins for Auto-Refresh

1. Open **Settings (Binance USDM Latest 1m Auto-Refresh)** → clear the filter → click **Select visible** → **Save**
2. Open **Settings (Latest 1m Auto-Refresh) — Hyperliquid** → clear the filter → click **Select visible** → **Save**
3. Switch exchange dropdown to **Bybit** → open **Settings (Bybit Latest 1m Auto-Refresh)** → clear the filter → click **Select visible** → **Save**
4. Repeat for **OKX** and **Bitget** if you use their local 1m datasets.

This registers all coins for the rolling update loop. The loop will keep the last few days current automatically — no further manual action needed after the initial backfill.

Settings API responses include an additive `apply` object with `timing`,
`restart_required`, and a safe `message`. Latest-1m settings apply on the next
PBData cycle; archive paths and TradFi profiles are read by the next operation.
No global Market Data settings watcher or service restart is implied.

### Step 2 — Run "Build best 1m all" for the initial backfill

Go to **Build best 1m OHLCV** and click **Build best 1m all** (or select all coins and submit).

This queues one background job per exchange that downloads the complete history from inception:

| Exchange | Download method | Expected duration (first run) |
|---|---|---|
| **Binance** | Parallel monthly + daily ZIPs (data.binance.vision) + CCXT fill | ~2–4 hours (~550 coins) |
| **Bybit** | CCXT (async) | ~3 hours (~550 coins) |
| **OKX** | Official archive ZIPs plus REST repair | depends on selected coins and history |
| **Bitget** | REST-only `USDT-FUTURES`, optionally distributed | ~7–9 minutes for full BTC history on one downloader |
| **Hyperliquid** (crypto) | l2Book archive + 1m\_api conversion | depends on l2Book archive size |
| **Hyperliquid** (XYZ stock-perps) | Tiingo IEX/FX 1m | depends on number of mapped symbols + Tiingo quota |

**Benchmarks from actual runs:**
- Binance LINK (6+ years, 2 239 days, 74 monthly ZIPs): **41 s** with parallel ZIP download
- Binance all ~550 coins (parallel ZIPs): **estimated 2–4 h** (extrapolated: avg. coin ~3 years ≈ 24 monthly ZIPs → ~20 s/coin)
- Bybit all 548 coins (CCXT, observed): **~3 h** (BTC alone = 102 min, short coins add proportionally little)
- Bitget BTC from inception (REST-only, observed): **~8 min 39 s** for 3.66 million candles; exchange-wide duration depends on selected coins and optional downloaders.

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
- OKX: latest **2–7 days** are refreshed every 3 600 s (1 h) per cycle
- Bitget: latest **2–7 days** are refreshed from public REST every 3 600 s (1 h) per cycle
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
1. Settings (Latest 1m Auto-Refresh) for the selected exchange
2. Market Data status for the selected exchange
3. Build best 1m OHLCV
4. TradFi Symbol Mappings
5. Download l2Book from AWS (Hyperliquid only)

## Market Data Page

The `Market Data` page now runs directly on the FastAPI implementation, and the sidebar exposes the settings area through three dedicated subsections:

The sidebar itself is now navigation-only: it contains the main page sections plus the contextual `Settings` actions, without separate overview or status summary info boxes.

- `Coin Refresh` — exchange refresh settings and the enabled-coins workflow
- `AWS / l2Book` — Hyperliquid archive download settings
- `TradFi / Tiingo` — server-side Tiingo profile status and TradFi mapping controls

The shared `Guide` button on that page opens this `Market Data` topic directly inside the page overlay, so the current Market Data view stays visible while you read.

The sidebar no longer shows a separate `Actions` section. Instead it exposes direct shortcuts that stay inside the page:

- `OHLCV Data` stays inside FastAPI too: when that panel is active, the sidebar reveals dataset buttons for the selected exchange instead of in-panel tabs.
- `Build Best 1m` opens a dedicated FastAPI panel for the current exchange.
- `Download l2Books` opens the embedded Hyperliquid data-actions panel directly when `Hyperliquid` is selected.

`Build Best 1m` and `Download l2Books` now also use the same active button highlight as the other Market Data sidebar entries, so the currently open shortcut section is visible directly in the sidebar.

Inside that FastAPI `Best 1m` panel, Hyperliquid reuses the full download/build actions component in a focused way: `Best 1m` shows only the build content, and `Download l2Books` shows only the download content. The extra outer header card, nested window chrome, and the expander header itself are removed there so only the actual form content remains visible.

Hyperliquid `Best 1m` now also matches the newer FastAPI editing patterns more closely: the build range uses the same editor-style popup calendar as the Backtest/Optimize editors, and the coin chooser is rendered as a multi-column enabled-coins grid with `Filter enabled coin list`, `TradFi only`, `No downloaded history`, `Select visible`, and `Clear all` instead of the old compact dropdown. XYZ build options are limited to canonical mappings with a usable Tiingo or Tiingo FX ticker; `pending`, `no_provider`, and `delisted` symbols are not offered because the downloader cannot fetch them. For XYZ coins, `No downloaded history` keeps symbols without any Tiingo-backed `other_exchange` minute in the local source index, so a current Hyperliquid day alone does not hide a coin that still needs its historical backfill. Both toggles can be combined with the text filter. The visible coin rows are directly clickable now and also support mouse-drag selection so larger ranges can be marked or cleared without checkbox clicking. Fast drag moves now interpolate the rows between cursor updates as well, so quick paint-style selection no longer skips coins.

Hyperliquid `Download l2Books` now uses that same coin-grid pattern too instead of the old compact dropdown. You can filter the enabled coin list, click visible rows directly, bulk-select the current filtered slice, clear the explicit selection, or drag across the visible grid to paint larger download ranges quickly. `XYZ-*` / TradFi symbols are excluded there because Hyperliquid l2Book archive downloads only apply to native coins. Leaving the selection empty still queues all remaining downloadable coins.

The focused Hyperliquid panel now also re-fits its embedded height when you switch between `Best 1m` and `Download l2Books`, so the shorter download view no longer keeps the empty tail and extra scrollbar from the previously taller build view.

The embedded Hyperliquid view also avoids a second internal page scrollbar now, so scrolling stays on the main Market Data page instead of splitting between the page and the focused panel.

For Binance, Bybit, OKX, and Bitget, the coin chooser uses a settings-style available-coins grid directly in the FastAPI panel: `Filter available coin list` narrows the grid, `Select visible` adds the current filtered slice, `Clear all` resets the explicit selection, and you can drag across the visible coin rows with the mouse to add or remove larger ranges quickly. Fast drag movement fills the intermediate rows too. Leaving the selection empty queues all available coins, while any explicit selection limits the Best 1m job to exactly those coins.

That FastAPI `Best 1m` view starts directly with the build fields for Binance, Bybit, OKX, and Bitget. The redundant intro header text and the extra top `Refresh` button were removed.

For Binance, Bybit, OKX, and Bitget, the FastAPI `Best 1m` build panel shows the filtered Job Monitor directly below the full build form, so you can watch queued, running, done, and failed jobs for the selected exchange without leaving the panel.

That build area is flatter now as well: the coin/build section no longer sits inside an extra rounded card frame, and the embedded Job Monitor drops its standalone page chrome so the whole view reads as one continuous Market Data panel.

That embedded Job Monitor now also grows with its own content height, so you no longer get a second scrollbar inside the monitor area while the outer Market Data page is already scrollable.

The embedded monitor URL now carries the current PBGui serial as a cache-buster, so frontend updates also refresh the iframe itself and new monitor actions such as `View` show up immediately without staying on an older cached copy.

Hyperliquid uses its own inline data-actions page instead of that shared iframe, and that inline Job Monitor now also includes the same `View` action for active, done, and failed jobs so the details modal is consistent across Market Data and `System -> Services`. Pending rows in both monitor variants now also expose `Run`, which requests one extra manual same-type parallel slot so one selected pending job can start alongside the already running job of that type. Active rows stay in stable queue/start order now as well, so live progress updates no longer reshuffle two running jobs back and forth. `View` and `Log` dialogs in both variants are capped to the visible browser viewport too, and they now follow both the browser scroll position and clipping parent panels such as the scrollable `Build Best 1m` container, so their close button stays inside the actually visible monitor area instead of opening above it.

Both monitor variants render job payloads, progress values, and backend errors strictly as text while retaining the same cards, buttons, dialogs, and expanders. Unexpected values from persisted jobs or external error messages cannot become executable page markup.

Its action dialogs are styled in-page now as well: cancel, delete, retry, requeue, and bulk-delete confirmations no longer fall back to browser-native popup windows.

The FastAPI `OHLCV Data` panel now keeps the full data-review workflow in one place. The selected exchange gets dataset buttons directly in the sidebar: `1m`, `PB7 cache`, and the separate `PB8 cache` inventory are always available, while Hyperliquid also shows `1m_api` and `l2Book`. The main panel then shows summary metrics, a filterable inventory table, deletion tools for writable datasets, a coverage heatmap, a minute heatmap when available, and an optional OHLCV detail chart. Both runtime cache inventories are read-only and use separate SQLite inventory keys even when exchange, timeframe, and coin names match.

That FastAPI OHLCV detail chart now uses lazy zoom loading. The initial iframe only ships coarse layers, so long histories open reliably again, and wheel zoom pulls finer candles on demand instead of trying to embed the full `15m` / `5m` / `1m` pyramid up front.

The iframe template itself is now served as real HTML/JS again, so the chart no longer stalls on a blank `Loading chart...` panel because of escaped quote characters inside the embedded script.

In Hyperliquid `OHLCV Data` → `l2Book`, the toolbar next to `Select All` / `Deselect` now also exposes a default-off toggle to include enabled non-XYZ coins that still have no l2Book files at all. That makes it possible to spot coins with completely missing l2Book coverage directly in the inventory table instead of only seeing coins that already have at least one archived hour.

The `OHLCV Data` sidebar stays button-only now. `Delete older than` was replaced by `Delete by Date`; clicking it opens a small dialog with the cutoff date picker and the delete preview instead of embedding that extra input block permanently in the sidebar.

That dialog now also mirrors the clearer Backtest editor date control pattern more closely: the cutoff field has a visible calendar button, and the current delete scope shows the selected coin names in a small scrollable list so multi-coin deletes stay explicit before you confirm them.

The final delete confirmation now also stays inside the PBGui styling: instead of the browser-native popup, delete actions open a centered confirmation window with the current scope and selected coins when applicable.

When you select one or more coins in `OHLCV Data`, the sidebar exposes the queue action that matches the current dataset view. In `1m`, `1m_api`, and `PB7 cache`, that remains `Build best 1m` for the selected coins on the current exchange. In Hyperliquid `l2Book`, the sidebar instead exposes an l2Book download queue action for those selected coins, so the inventory view no longer offers the unrelated Best 1m job there. The inventory sidebar itself is now button-only: queue/delete confirmations and errors no longer stay in persistent sidebar callouts, but go through the normal toast/notification path or the existing confirmation dialogs instead. The visible coin labels in this inventory UI now use the short coin name only, including the table, sidebar action buttons, and the heatmap/OHLCV captions.

In `PB7 cache` and `PB8 cache`, the toolbar above the table also includes a small timeframe quick filter next to `Select All` and `Deselect`. Use it to switch between `all`, `1m`, and `1h` rows before selecting coins, which avoids the short-name duplicates that appear when the same coin exists in both cached timeframes.

In Hyperliquid inventory views, the type filter now also supports `xyz only`, `xyz mapped`, and `xyz not mapped`. The table shows a `mapping` column for Hyperliquid rows, so you can immediately see the effective TradFi mapping status for each visible XYZ instrument, including statuses such as `mapped`, `no provider`, or `pending`. Active XYZ instruments are no longer shown as `delisted` just because an old entry in `tradfi_symbol_map.json` was not refreshed yet; when the live Hyperliquid mapping still lists the symbol, PBGui now resolves an active non-delisted status instead.

The inventory table now also uses the same mouse-selection behavior as the FastAPI Backtest/Optimize tables: clicking toggles a single row, dragging across rows adds or removes a contiguous range, and `Select All` only selects the rows that are currently visible after filtering.

The inventory table headers are sortable as well. Clicking a column header toggles between ascending and descending order for the currently visible rows in that dataset view.

## Copy Data

Use **Copy Data** to copy local OHLCV files from this PBGui `data/ohlcv` tree to another PBGui host over SSH with `rsync`.

- **SSH command without target** — the SSH command used as rsync's remote shell. Do not include the final target here. Supported forms are `ssh`, `ssh -p 2222`, `ssh -J user@jump-host`, and `ssh -J user@jump-host -p 2222`; shell-capable options such as `-o ProxyCommand` are rejected.
- **Remote target** — the final SSH target used by rsync, for example `user@target-host`, `target-host`, `localhost` for a reverse tunnel, or an SSH config alias.
- **Destination data/ohlcv root** — absolute `data/ohlcv` root on the target host. Leave empty when the target PBGui uses the same path as this machine.

Copy jobs update new and changed files. They never use `--delete`, so files that only exist on the optimizer system remain untouched.

Click **Test connection** first to run a read-only SSH and destination-path check. It does not create directories and does not copy files.

Click **Dry run** before the real copy when you want to verify the exact target path and estimated rsync transfer. The dry run queues a background job with `--dry-run --stats --itemize-changes`; it skips remote `mkdir`, writes no files, and records the per-exchange rsync stats in the embedded Copy Job Monitor log.

### Copy Schedules

Use **Copy Schedules** to keep one or more optimizer systems current automatically. The schedule stores the current SSH command, remote target, destination root, and exchange selection.

- Enter a schedule name and an interval from 1 to 168 whole hours.
- Enable the schedule and click **Save schedule**. Its first automatic copy starts after one complete interval.
- Use **Run now** for an immediate copy from the saved settings.
- Use **Edit** to load a schedule back into the Copy Data form, change its target, exchanges, interval, or enabled state, and save it again.
- Scheduled copies are persistent across API restarts. A schedule never starts a second copy while its previous job is still pending or running.

Each automatic or manual schedule run appears in the same embedded Copy Job Monitor as a regular copy job. Deleting a schedule is blocked while its own copy job is active; the detached copy worker itself continues safely across an API restart.

## Settings (Latest 1m Auto-Refresh) — Hyperliquid

Controls the automatic 1m candle refresh loop for Hyperliquid symbols.

- **Enabled coins** — multiselect from all known Hyperliquid symbols
- **Clear filter + Select visible / Clear all** — quickly enable or disable all coins
- **Cycle interval (s)** — how often all enabled coins are refreshed (default: 1800s)
- **Pause between coins (s)** — delay between coins to avoid rate limits (default: 0.5s)
- **API timeout per coin (s)** — per-coin request timeout (default: 30s)
- **Min / Max lookback days** — window for the latest fetch (default: 2 / 4 days)
- Changes are saved to `pbgui.ini` and applied in the next cycle — no restart needed.

Hyperliquid latest-1m catch-up requests can now reserve the full configured 4-day `candle_snapshot` budget correctly. A previous burst-cap mismatch in the local rate limiter could force repeated `budget_timeout` results even when the API request itself was valid.

## Settings (Binance USDM Latest 1m Auto-Refresh)

Controls the automatic 1m candle refresh loop for Binance USDM perpetuals.

- **Enabled coins** — multiselect from all known Binance USDM coins
- **Clear filter + Select visible / Clear all** — quickly enable or disable all coins
- **Cycle interval (s)** — how often all enabled coins are refreshed (default: 3600s)
- **Pause between coins (s)** — delay between coins (default: 0.5s)
- **API timeout per coin (s)** — per-coin request timeout (default: 30s)
- **Min / Max lookback days** — window for the latest fetch (default: 2 / 7 days)
- Changes are saved to `pbgui.ini` and applied in the next cycle — no restart needed.

Multiplier-prefixed contracts keep their short coin name when they are the only market for that asset, for example `1000SHIB` remains `SHIB`. If the same exchange and quote list both contracts, PBGui keeps them separate: `1000CAT` is Simon's Cat and `CAT` is the unprefixed Caterpillar market on Binance and Bitget. Existing Market Data selections are migrated from the old ambiguous `CAT` name to `1000CAT`; select `CAT` separately if you also want the unprefixed market in automatic refreshes or Best 1m builds.

## Settings (Bybit / OKX / Bitget Latest 1m Auto-Refresh)

These exchanges use the same enabled-coins, cycle interval, pause, API timeout, and min/max lookback controls. Defaults are a 3,600-second cycle with a 2–7 day lookback. Changes apply on the next PBData cycle without a restart.

For Bybit, every completed 24x7 market day in that window is verified for all 1,440 contiguous minutes before refreshed files are written. A failed pagination or incomplete closed day is discarded without replacing existing data, and the next hourly cycle retries it. Overwrites also replace the source coverage for that day so the heatmap cannot retain stale minute availability.

The automatic Bybit cycle refreshes only the running UTC day. On the first cycle at or after `00:15 UTC`, PBData finalizes the previous day for each enabled coin. A successful result is recorded in `data/ohlcv/checksums.sqlite` and survives restarts; only failed coins retry on later hourly cycles.

Bitget lists active USDT linear swaps only. Its latest refresh and historical Best 1m backfill use the public `USDT-FUTURES` REST candle endpoints. Worker threads within one local download share an 18 req/s limiter and back off together on rate limits; avoid starting multiple Bitget downloads concurrently because the exchange limit applies to the public IP. There is no Bitget archive fallback. A local non-distributed backfill re-requests incomplete historical days and reports minutes Bitget cannot supply.

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

For Bybit, the status also separates the current-day refresh from daily finalization with `current_result`, `current_minutes_written`, `finalization_day`, `finalization_result`, and `last_finalized_day`.

### Restart Behavior

After a restart, PBData starts a new cycle after its normal startup offset. Completed Bybit finalizations are skipped using the persistent checksum catalog; failed or interrupted coins remain eligible for the next hourly attempt.

## OHLCV Integrity

Open **OHLCV Integrity** to inspect the daily checksum catalog for the exchange selected at the top of Market Data. Read-only scan, summary, grouped findings, checksums, and public-reference comparison support Binance USDM (`binanceusdm` storage), Bybit, OKX, Bitget, and Hyperliquid crypto. Hyperliquid XYZ/TradFi directories are excluded until session-aware validation is implemented.

The initial Bybit scan is queued automatically after an update. Scans for Binance USDM, OKX, Bitget, and Hyperliquid crypto are deliberately manual in this first rollout because the combined archive contains roughly 1.8 million daily files. Select an exchange and use **Run full scan**; integrity scan jobs are serialized. Until a scan completes, that exchange shows `Pending`. Non-Bybit catalogs are point-in-time snapshots and must be rescanned after their underlying files change.

Repeated full scans reuse catalog results for files whose nanosecond modification time and size are unchanged. Those files are still discovered so deleted or newly added days remain visible, but their NPZ payload is not reopened or rehashed. Changed files and days whose inception/current-day classification may have changed are always validated again.

For each coin, a contiguous suffix in its earliest local daily file is classified as `inception_partial`; the scanner does not synthesize missing dates before the first local file. Later partial days and internal gaps remain invalid. A newest partial closed day is not accepted merely from its local position.

Findings are grouped by coin with the damaged-day count, date range, missing minutes, and validation reasons. All five exchanges offer **Repair coin** and **Repair all**. Bybit uses its exact-day finalizer; Binance USDM, OKX, and Bitget refetch only the requested damaged day through their normal builders. Hyperliquid repair improves that exact day from available Hyperliquid API/L2Book data and then fills remaining minutes from Binance followed by Bybit. Every path recalculates the checksum and retains a finding when the result is still incomplete.

Integrity validates the High, Low, Close, and Volume values used by Passivbot. Exchange Open values outside High/Low are accepted because Open is not stored in Passivbot's backtest representation. For Hyperliquid, **Normalize fallback candles** offers a confirmed maintenance job for historical `other_exchange` minutes created by the Cross-Exchange Fallback. It only expands High/Low to enclose the existing Open/Close; timestamps, Open, Close, Volume, gaps, and source assignments remain unchanged. Newly generated fallback candles are normalized automatically.

The table always loads all currently damaged days. **Repair all** runs them sequentially in one cancellable background job. A failure for one day is retained in the result and does not stop the remaining repairs. Summary cards and rows refresh automatically when an Integrity job finishes.

The Repair Queue uses one row per coin instead of repeating a coin for every damaged day. Each row summarizes its damaged-day count, date range, total missing minutes, and reason counts. **Repair coin** runs one sequential batch scoped to all damaged days for that coin.

Use **Details** to inspect each damaged day before repairing it. The minute-coverage view shows 24 hourly rows with one cell per minute: present candles are green, a leading range on the earliest local day is yellow as a possible exchange-inception boundary, internal gaps are red, and trailing gaps are orange. A day selector is available when the grouped coin has multiple findings, and the range table lists exact UTC start/end times and minute counts.

Above the minute chart, **Surrounding Days** shows seven days before and seven days after the selected date as compact 24-hour coverage rows. Complete hours are green, partial hours orange, and missing hours red. Any cataloged neighboring day can be clicked to inspect its full minute coverage, including valid days that are not part of the Repair Queue.

The Repair Queue's **Missing** count excludes a leading range on the earliest local coin day because trading may simply have started later than `00:00 UTC`. The detail view still displays those absent minutes in yellow. Only missing minutes from the first available candle through `23:59 UTC` count as damaged.

Repair All retries transient network/time-out failures once. Bybit independently verifies exchange inception from the exact current instrument's `launchTime` when local damaged files would otherwise obscure it. Binance applies the equivalent check from the exact current market's `onboardDate`, because Binance may reuse a symbol while still serving the previous instrument generation through its archive and API. If either exchange confirms a newer inception, **Repair coin** or **Repair all** automatically removes the complete obsolete local generation before that date, including its source-index coverage and checksum rows, while preserving all days from the confirmed inception onward. Future Binance full builds also start at that current `onboardDate`, so the old generation is not downloaded again. The job result reports removed days separately. The Binance BTC USDT futures launch day at `2019-09-08 19:00 UTC` and Bybit XTZ at `2021-01-11 05:50 UTC` each have one independently confirmed source-native gap; PBGui records only those exact minutes as `source_gap` rather than fabricating candles or repeatedly attempting impossible repairs. If any days still fail, successful repairs remain saved but the batch is shown under **Failed** with its partial result instead of appearing fully successful under Done.

Hyperliquid exact-day repair checks both local and configured archived/NAS L2Book hours before running the normal Binance-then-Bybit fallback. It also expands High/Low around Open/Close for any existing historical `other_exchange` candle on that exact day before final validation. If the historical day remains incomplete, PBGui compares every exact missing minute with the current donor instruments' local `onboardDate`/`launchTime` metadata. Minutes proven to predate both donors, including native PURR history without a matching donor, are cataloged as `source_gap`. If metadata says a donor may cover a minute, PBGui requires successful exact-day queries to both donors before accepting that the minute is absent from their retained history; a network or exchange error keeps the day damaged and repairable. Source-gap rows leave the Repair Queue but remain visible in the separate **Source gaps** summary count. The status is bound to the unchanged NPZ fingerprint or continued absence of a complete day, so new or modified data is automatically validated strictly again. PBGui never creates carry-forward candles for this classification.

The **Unavailable Coin Data** table is scoped to the selected exchange and lists every local coin absent or inactive in its current `mapping.json`, including coins whose existing files are still valid. **From** and **To** show the earliest and latest local daily OHLCV files for each market. Click rows or drag across rows to select a range, then use **Remove selected** or the `Delete` key. **Remove all** selects every currently removable unavailable market. One server-side preview revalidates the complete selection and shows its market count, file count, size, and date range before the required confirmation. A single persistent batch job then rechecks every market immediately before deleting its complete PBGui OHLCV raw data and source index; individual failures do not skip later markets and remain visible in the job result. Unsafe rows are excluded from Remove all. Removed markets are excluded from the Repair Queue. PB7 and PB8 runtime caches are not removed. **Repair all** skips unavailable markets and reports their count separately.

Checksum sharing uses the configured Config/Optimize archives:

- **Publish checksum snapshot** enables one daily publication after Bybit finalization completes and all five exchange scans have completed.
- **Publish archive** accepts only the configured writable own GitHub archive with a server-side access token.
- **Reference archive** may independently select any configured public GitHub archive. Compare-only systems do not need a token or cluster membership.
- **Publish now** requires completed scans for all five exchanges, creates a consistent SQLite backup, compresses it as `checksums.sqlite.gz`, and replaces the asset on the fixed `checksums-latest` GitHub release.
- **Refresh reference** anonymously downloads that release asset, validates it, retains the previous good copy on failure, and compares it read-only with the local catalog.

The checksum snapshot contains only OHLCV identifiers, daily counts, validation states, timestamps, and content hashes. Archive credentials remain server-side and are never included in Market Data task payloads, release URLs, or logs. The selected reference repository must be public for anonymous downloads.

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
- Auto-Map and metadata/price refresh require an active Tiingo vault profile. Reveal, configure, or replace its token directly under **Settings -> TradFi / Tiingo**, or manage advanced profile metadata under **Setup -> API Keys -> TradFi**. Bulk status and settings responses remain secret-free.

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

On the FastAPI page, Binance USDM, Bybit, OKX, and Bitget use a settings-style available-coins grid directly in the `Best 1m` build panel. You can narrow the list with `Filter available coin list`, click single rows, drag across visible rows to add or remove larger ranges quickly, or bulk-add the current filtered slice via `Select visible`. If you leave the explicit selection empty, PBGui queues all available coins for the current exchange.

On Hyperliquid, the focused `Best 1m` build panel now uses the same `Filter enabled coin list` + multi-column grid pattern for coin selection and the shared popup calendar style for `Start date` / `End date`, replacing the older single-row dropdown and browser-native date fields. The visible coin rows can be clicked directly or selected in larger ranges by dragging the mouse across the grid.

### Job Types

**`hl_best_1m`** — Hyperliquid XYZ stock-perps:
- Eligibility: mapping status `ok` + Tiingo ticker present
- Controls: Build best 1m, Start date, End date, Refetch TradFi from scratch

**`binance_best_1m`** — Binance USDM full historical backfill:
- Downloads complete inception-to-today 1m OHLCV from official Binance archives (data.binance.vision) — monthly + daily ZIPs — with CCXT gap-fill
- Coin selection from all available Binance coins
- Controls: Start date, End date, Refetch
- Storage: `data/ohlcv/binanceusdm/1m/<COIN>/YYYY-MM-DD.npz` (compressed NumPy archive; PB7 cache uses uncompressed `.npy` — ~35% larger for the same data)

**`bybit_best_1m`** — Bybit REST/CCXT historical backfill for available coins.

**`okx_best_1m`** — OKX archive backfill with REST repair for missing candles and volumes.

**`bitget_best_1m`** — Bitget USDT-FUTURES REST-only historical backfill:
- Uses Bitget symbols from `data/coindata/bitget/mapping.json` and writes `data/ohlcv/bitget/1m/<COIN_DIR>/YYYY-MM-DD.npz`.
- A local non-distributed backfill validates complete historical days against 1,440 minutes; the listing day and current UTC day may remain partial.
- Optional **Distributed download** splits missing date ranges across selected VPS downloaders and/or the master. Remote downloaders stream raw candles back; only the master writes NPZ and source-index files. Later runs plan incomplete middle historical days again even while `Refetch` is off; listing/range-start and current UTC boundary days remain partial-day exceptions.

### Job Management

The job panel shows three sections:
- **Pending** — jobs queued for execution
- **Running** — currently executing job with live progress
- **Failed / Done** — completed jobs

Focused Best 1m history tabs apply the selected job type before the history limit, so unrelated high-frequency jobs do not hide completed or failed jobs for the current exchange action.

An API restart can stop the separate Market Data Queue controller through the service cgroup. Any non-cancelled running job is moved back to **Pending** with its persisted progress intact, and the reconstructed worker resumes it from the data already written instead of marking it as failed. Explicit cancellation and genuine processing errors still move jobs to **Failed**.

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

## Tiingo Credentials

Reveal, create, or replace the Tiingo token directly under **Settings -> TradFi / Tiingo**, or manage advanced profile metadata under **Setup -> API Keys -> TradFi**. Both paths use the same credential vault. The input is never prefilled; the stored key is requested only after an explicit click on the eye and is cleared when hidden or when the page is left. Do not add Tiingo to `pbgui.ini` or edit PB7 TradFi entries manually.

This page provides an explicit reveal and secure create/replace input for the active Tiingo vault token, runtime quota indicators (hour/day/month bandwidth), provider links, and mapping tools that use the active vault profile. Profile lists and settings responses remain secret-free.

## Troubleshooting

If a build job appears briefly and disappears:
1. Check the latest failed job in `data/ohlcv/_tasks/failed`
2. Confirm worker is running the latest code (restart worker if needed)
3. Verify the Tiingo vault-profile and symbol-mapping status
4. Use `Test Resolve` for the selected symbol

If Build coin list is empty:
- Ensure symbols are mapped and status is `ok`
- Ensure Tiingo ticker or FX ticker exists in mapping
