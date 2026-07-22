# PBv7 Backtest

The **PBv7 Backtest** page lets you create, run and evaluate Passivbot v7 backtests.
It is a standalone FastAPI page — no page reload is needed. Real-time queue updates arrive via WebSocket.
Draft handoffs from the FastAPI **Run** and **Optimize** pages now open here directly as FastAPI drafts as well, so switching between those PBv7 pages no longer needs a legacy relay path.
PBv8 Backtest renders this same page template and visual editor; a small version adapter changes only generation-specific config paths and API endpoints.

The page is organised into five panels selected from the left sidebar:

| Panel | Purpose |
|-------|---------|
| **Configs** | Create and edit backtest configurations |
| **Queue** | Monitor and control the backtest runner |
| **Results** | Browse and analyse completed backtest results |
| **Archive** | Access community and personal config archives |
| **Legacy** | Browse old result folders found under `pb7/backtests` outside the PBGui-managed `pbgui` path |

The **top navigation bar** contains:

| Button | Action |
|--------|--------|
| 🔔 | Open the notification log (floating panel showing `PBV7UI.log`) |
| 📖 Guide | Open this help page |
| ℹ️ About | Show PBGui version info |

---

## Configs panel

### List view

The table shows all saved backtest configurations with columns:
**Name**, **Exchange**, **Start Date**, **End Date**, **Created**, **Modified**, **Actions**.

**Selection:** Click a row to toggle selection. Hold and drag to select a range.
Use the **Select All** / **Deselect** buttons above the table.

**Sidebar actions:**

| Button | Action |
|--------|--------|
| **+ New Config** | Create a new backtest configuration |
| **🗑 Delete Selected** | Delete the selected configs (confirmation dialog with option to also delete results) |

Double-click a row to open it in the editor.

### Edit view

Editing opens inline in the main area. Fields:

| Field | Description |
|-------|-------------|
| **Name** | Config name (used for results and queue display) |
| **Exchange(s)** | One or more exchanges to run the backtest on |
| **start_date / end_date** | Date range for the backtest |
| **starting_balance** | Initial balance in USD |
| **hsl_signal_mode** | PB7-derived selector for account-level HSL behaviour: `pside` keeps long/short drawdown signals separate, `unified` shares one combined signal |
| **logging_level** | Run-style selector for `warning`, `info`, `debug`, and `trace` verbosity |
| **approved_coins / ignored_coins** | Explicit coin lists; populated automatically by **Apply Filters** |
| **Coin sources** | Where coin lists come from (PBGui coin database, manual, etc.) |
| **Market settings sources** | Source for market-specific settings |
| **Bot parameters** | Strategy parameters (long/short side, TWE, etc.) |

**Editor action buttons:**

| Button | Action |
|--------|--------|
| **💾 Save** | Save the configuration to disk |
| **← Back** | Return to the config list without saving |
| **Add to Queue** | Save and enqueue → switches to Queue panel |
| **Apply Filters** | Populate approved/ignored coin lists from the current filter settings |
| **📊 View Results** | Jump to this config's results in the Results panel |
| **⏩ Convert to V8** | Convert the currently saved V7 config with PB8's official migrator and open it in PBv8 Backtest; disabled until the config has been saved |
| **💰 Balance Calculator** | Open the shared Balance Calculator under Information with the current editor config loaded as a draft |
| **⚡ Calc Balance** | Run the same balance calculation inline in a modal without leaving the Backtest page |
| **🧭 OHLCV Readiness** | Open a draggable, resizable floating window and run a PB7-backed read-only preflight for the current config, showing whether each approved coin is locally ready, can import from legacy OHLCV data, would fetch on start, or is blocked by persistent gaps; the list evaluates the union of `approved_coins_long` and `approved_coins_short`, and each entry now shows whether it comes from `long`, `short`, or both. If PB7 would fetch missing ranges, the window also offers **Preload OHLCV Data** to warm the cache in the background before starting the backtest, automatically jumps to the preload job log section when that preload starts, shows real log-derived progress rows from the active archive/ccxt download lines plus duration, PID, log counters, and last-update details, follows CCXT progress via the moving request cursor instead of bouncing to 100% when an exchange returns newer candles than requested, uses the same warmup-adjusted effective start as the readiness check so the post-preload refresh no longer leaves the warmup days behind, classifies markets that only launched after the requested window as too young instead of pretending those older candles can be fetched, prunes such coins from preload jobs, includes a **Stop Preload** action while the downloader is active, provides a top-right fit-to-browser-window control for the floating panel, keeps that log tail running without jumping back to the top, and keeps the finished preload result visible until a fresh readiness check replaces it |
| **📥 Import** | Open the Run-style paste-JSON dialog and load the imported config into the editor for review; pasted configs are normalized through the same PB7 load pipeline as regular saved configs, so supplemented parameters and `neutralized` / `review` markers are preserved |

The **Raw JSON** expander, the **Bot Configuration** `long` / `short` JSON editors, JSON-based **Additional Parameters**, and the **Import** dialog now use shared JSON validation. Invalid JSON is highlighted directly in the editor, the faulty line can be revealed with a button, the error hint appears in one shared fixed viewport location, and saving/importing is blocked until the JSON is valid again.

The **Coin Overrides → Config File** `long` / `short` JSON editors use the same JSON validation pattern and the same shared fixed viewport error hint location as the main editor fields. Invalid JSON is highlighted directly in the editor, and closing the coin override editor is blocked until those JSON snippets are valid again.

### Coins & Filters

These fields control which coins are included via PBGui's coin database.
After adjusting them click **Apply Filters** to update the approved/ignored lists.

| Field | Description |
|-------|-------------|
| **market_cap (min M$)** | Minimum market cap in millions USD. Set to `0` to disable. |
| **vol/mcap** | Max 24h volume-to-market-cap ratio. Very high ratios often indicate low-quality coins. |
| **tags** | CoinMarketCap category tags. Only coins with at least one matching tag are included. Empty = all. |
| **only_cpt** | Include only copy-trading eligible coins. Requires fresh copy-trading data (Coin Data page). |
| **notices_ignore** | Exclude coins with active CoinMarketCap notices (e.g. investigation, insolvency). |

---

## Queue panel

Shows all pending, running and finished backtest jobs with live status updates.

### Table columns

| Column | Description |
|--------|-------------|
| **Status** | `queued` / `running` / `backtesting` / `complete` / `error` |
| **Name** | Config name |
| **Exchange** | Exchange(s) used |
| **Created** | Timestamp when the job was enqueued |
| **Actions** | Context-sensitive action buttons |

**Selection:** Click a row to toggle, drag to multi-select.
Use the **Select All** / **Deselect** toolbar above the table.

### Per-row action buttons

| Button | Condition | Action |
|--------|-----------|--------|
| ▶ (yellow) | `error` | Restart — immediately relaunch the failed backtest |
| ▶ (default) | `queued` | Start — launch this job immediately |
| ⬛ (red) | `running` / `backtesting` | Stop — kill the running process |
| 📊 (green) | `complete` | View Results — switch to Results panel filtered to this config |
| 📜 | always | Log — open a floating log panel for this job's log file |
| 🗑 | always | Remove — delete this queue entry (stops if running) |

### Sidebar actions

| Button | Action |
|--------|--------|
| **📈 Compare** | Load the matching results for the selected completed queue jobs, switch to Results, and open the comparison chart directly |
| **✓ Clear Finished** | Remove all `complete` and `error` jobs |
| **⬛ Stop All** | Kill all running backtest processes |
| **🗑 Delete Selected** | Remove selected queue entries |
| **⚙ Settings** | Open the Settings modal |

When you select multiple completed queue rows and click **📈 Compare**, PBGui resolves the matching result batch for each selected queue item, opens the **Results** panel, preselects those result rows, and renders the comparison chart immediately. Queue items that are not complete yet or have no matching stored result are skipped.

### Settings modal

PB7 and PB8 use one shared queue settings configuration. Saving it on either Backtest page updates both workers. The CPU value is one global automatic PB7/PB8 process limit, not a separate allowance per version. The dialog renders immediately from its current state and refreshes authoritative host values in the background without overwriting edits.

The queue settings dialog also includes `Use PBGui Market Data`. When that setting is enabled, PBGui rewrites `backtest.ohlcv_source_dir` to the current PBGui market-data root immediately before each queued or manual backtest launch, regardless of the path stored in the config editor.

| Setting | Description |
|---------|-------------|
| **CPU** | Global number of automatic PB7/PB8 backtest processes (max = CPU core count) |
| **Autostart** | When enabled both version workers automatically pick up `queued` jobs within the shared CPU limit |
| **Use PBGui Market Data** | Overrides `backtest.ohlcv_source_dir` right before launch so queued jobs always use the PBGui-managed OHLCV dataset |
| **HLCVS Cache Cleanup — Enabled** | Periodically clean the version-specific PB7 and PB8 cache roots |
| **Retention (days)** | Delete directories older than this many days (default: 7) |
| **Check interval (h)** | How often the cleanup runs in hours (default: 24) |
| **🧹 Clean Now** | Run cleanup immediately for the currently open PB7 or PB8 page runtime |

---

## Results panel

Browse all completed backtest results.

### Filters & sort

- **Version** dropdown — show PBv7 results, PBv8 results, or both; PBv7 is selected by default on this page
- **Config** dropdown — filter by config name (exact match)
- **Search** text field — free-text filter on any column
- Click any column header to sort; click again to reverse

Completed queue jobs now invalidate the cached Results list immediately. If you are already on the Results panel when a backtest finishes, PBGui refreshes the table automatically so the new result appears without having to leave and reopen the panel.

### Toolbar actions

| Button | Action |
|--------|--------|
| **🔄 Backtest** | Re-run selected results as new backtests (opens date/balance/exchange modal) |
| **▶ Add to Run** | Create a live run from the selected config |
| **📈 Compare** | Add selected results to the comparison view |
| **🧬 Optimize from Result** | Open the Optimize editor directly with the selected result as draft and `Starting Seeds = self` |
| **🗑 Delete Selected** | Delete selected results from disk |

### Per-row actions

| Icon | Action |
|------|--------|
| 📊 | Open the result charts (equity curve, TWE, etc.) |
| **V8** | Convert this result's exact `config.json` with PB8's official migrator and open it in PBv8 Backtest |
| 🗑 | Delete this single result |

The Configs table also offers **V8** for the saved V7 backtest config. Both conversions leave the V7 source unchanged. When converting a result, PBGui derives the effective maker and taker rates from its linear-market `fills.csv` data before migration, preventing a normalized result default from replacing the exchange fees actually used by V7. PBGui removes only its own metadata and stale temporary loader path before migration; PB8 still blocks publication if real unsupported or manual-review fields remain.

### Result charts

Clicking a row opens a full-featured chart panel with:
- **Equity curve** (log scale toggle)
- **PnL** over time  
- **TWE** (total wallet exposure) chart  
- **Hedged PnL** if available  
- Full **analysis metrics** table  
- **Config JSON** viewer  

Use **📌 Pin** to keep the chart visible while browsing other results.
Use **📈 Compare** to overlay multiple results on one chart. With **Version: Both**, PBv7 and PBv8 results can be selected together; PBGui loads each equity file from its matching backend and labels the chart series with its version.

### Re-backtest modal

Available from the **🔄 Backtest** toolbar button. Options:

| Option | Description |
|--------|-------------|
| **start_date / end_date** | Override the date range for the re-run |
| **starting_balance** | Override the starting balance |
| **Exchange(s)** | Override which exchange(s) to use |
| **📂 Use PBGui Market Data** | When checked, sets `ohlcv_source_dir` to the PBGui-managed data path |

For archived results, these controls initially use the values stored in the archived `config.json`, including the end date and market-data choice. Clearing **Use PBGui Market Data** is an explicit override and is not replaced by the global Backtest setting when the queued job starts.

---

## Archive panel

Community and personal config archives stored as Git repositories. PBGui treats the archive selected as **My Archive** as writable. Other archives are read-only for content: you can browse, import, compare, re-backtest, and pull remote updates, but PBGui does not add, rename, delete, commit, or push their items. Pull is blocked before contacting the remote whenever a clone has local changes; push or otherwise resolve changes in **My Archive** first, while a dirty foreign clone remains untouched.

### Archive list view

| Button | Action |
|--------|--------|
| **⬇ Pull All** | Pull the latest commits from all configured archives |
| **⬆ Git Push** | Commit and push changes from **My Archive** to its remote |
| **+ Add Archive** | Clone a new archive by name and Git URL |
| **⚙ Setup** | Select **My Archive**, Git identity, token, auto-pull interval, and README text |
| **📋 Log** | Open the archive sync log in a floating panel |

Click an archive row to open it and browse its results. Counts come from `pbgui/archive_manifest.json` when available and fall back to a read-only filesystem scan when the manifest is missing or invalid.

PBGui derives archive destinations from the PB7 `config_version`; there is no manually editable archive path. Backtest results are stored below `pbgui/configs/{config_version}/backtests/`, and Optimize configs below `pbgui/configs/{config_version}/optimize/`. Missing or invalid versions use an `unknown` directory plus a content fingerprint to avoid collisions.

When **My Archive** is clean, PBGui migrates a bounded batch of legacy results when its panel is opened and checks for a full migration before adding or pushing content. A dirty worktree or failed Git status blocks migration without discarding existing changes. The status line reports whether legacy entries remain or migrated changes still need to be pushed.

### Archive content view

The view has **Backtests**, **Optimize Settings**, and, for **My Archive**, **Schedules** tabs.

| Button | Action |
|--------|--------|
| **🏠 Archives** | Return to the archive list |
| **🔄 Backtest** | Re-run selected configs as new backtests → switches to Queue |
| **▶ Add to Run** | Create a live run |
| **📈 Compare** | Add to comparison view |
| **🧮 Balance** | Open the selected result in the Balance Calculator |
| **🧬 Score Preview** | Preview archive scoring without writing the archive |
| **🗑 Delete Selected** | Remove selected results from **My Archive** only |

Additional **My Archive** actions include renaming a config group, **Retest & Replace**, rebuilding scores, compacting Git history, removing duplicates, and **Remove Liquidated**. Liquidated cleanup always shows a dry-run result and requires explicit confirmation before verified deletion. Scheduled retests can replace archived results only after a successful non-liquidated rerun.

The **Optimize Settings** tab can view or import archived Optimize configs from any archive. If the local name already exists, choose **Overwrite**, **Import as Copy**, or **Cancel**. Adding or deleting archived Optimize configs is restricted to **My Archive**. Re-exporting identical content reuses the existing fingerprint path and metadata instead of creating another numbered copy.

---

## Legacy panel

The **Legacy** panel is meant for old or misplaced result folders that exist under `pb7/backtests/*` but not under the normal PBGui-managed `pb7/backtests/pbgui/*` tree.

Use this panel when a backtest finished on disk but does not appear in the normal **Results** panel because it was written to a legacy location such as `pb7/backtests/combined/...`.

### Toolbar actions

| Button | Action |
|--------|--------|
| **↻ Refresh** | Re-scan legacy result folders |
| **🔄 Backtest** | Re-run selected legacy configs as new queued backtests |
| **▶ Add to Run** | Create a live run from the selected legacy config |
| **📈 Compare** | Overlay selected legacy results in a shared comparison chart |
| **🗑 Delete Selected** | Delete selected legacy result folders from disk |

### Notes

- The table supports the same row selection and drag-selection behavior as **Results** and **Archive**.
- Result names may be inferred from the directory path when the original config name is no longer available inside the legacy config.
- Use **🔄 Backtest** to move legacy runs back into the normal PBGui-managed workflow.

---

## Typical workflows

### Run a new backtest
1. **Configs** → **+ New Config** → fill in the config → **Add to Queue**
2. **Queue** → **⚙ Settings** → set CPU, enable **Autostart** → **Save**
3. Watch the status badge change from `queued` → `running` / `backtesting` → `complete`
4. Click 📜 on the job row to watch the live log in a floating panel
5. Click 📊 (green) when complete → jumps to Results

### Re-run / tune a result
1. **Results** → select a result → **🔄 Backtest** → adjust dates/balance → **OK**
2. **Queue** → monitor progress

### Turn a backtest result into an optimize draft
1. **Results** → select one result → **🧬 Optimize from Result**
2. PBGui opens the FastAPI Optimize editor directly, not the config list
3. The imported draft is preloaded with **Starting Seeds = self**, so the optimize run starts from that config itself

### Use a community config
1. **Archive** → **⬇ Pull All** → click into an archive → select configs → **🔄 Backtest**
2. **Queue** → monitor; or enable Autostart
3. After completion → **Results** to analyse

### Compare multiple results
1. **Results** → select results → **📈 Compare**
2. The comparison chart opens showing all selected equity curves overlaid

### Free up disk space (HLCVS cache)
1. **Queue** → **⚙ Settings**
2. Enable **HLCVS Cache Cleanup**, set **Retention** and **Check interval**
3. Click **🧹 Clean Now** for an immediate cleanup — the toast message reports freed MB
4. **Save** to persist the automatic schedule
