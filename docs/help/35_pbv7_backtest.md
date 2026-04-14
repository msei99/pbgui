# PBv7 Backtest

The **PBv7 Backtest** page lets you create, run and evaluate Passivbot v7 backtests.
It is a standalone FastAPI page — no page reload is needed. Real-time queue updates arrive via WebSocket.

The page is organised into four panels selected from the left sidebar:

| Panel | Purpose |
|-------|---------|
| **Configs** | Create and edit backtest configurations |
| **Queue** | Monitor and control the backtest runner |
| **Results** | Browse and analyse completed backtest results |
| **Archive** | Access community and personal config archives |

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
| **💰 Balance Calculator** | Open the standalone Balance Calculator page with the current editor config loaded as a draft |
| **⚡ Calc Balance** | Run the same balance calculation inline in a modal without leaving the Backtest page |
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
| **✓ Clear Finished** | Remove all `complete` and `error` jobs |
| **⬛ Stop All** | Kill all running backtest processes |
| **🗑 Delete Selected** | Remove selected queue entries |
| **⚙ Settings** | Open the Settings modal |

### Settings modal

| Setting | Description |
|---------|-------------|
| **CPU** | Number of parallel backtest processes (max = CPU core count) |
| **Autostart** | When enabled the worker automatically picks up `queued` jobs |
| **HLCVS Cache Cleanup — Enabled** | Periodically delete old `pb7/caches/hlcvs_data` directories |
| **Retention (days)** | Delete directories older than this many days (default: 7) |
| **Check interval (h)** | How often the cleanup runs in hours (default: 24) |
| **🧹 Clean Now** | Run the cleanup immediately using the current retention value; reports how many directories were removed and how much disk space was freed |

---

## Results panel

Browse all completed backtest results.

### Filters & sort

- **Config** dropdown — filter by config name (exact match)
- **Search** text field — free-text filter on any column
- Click any column header to sort; click again to reverse

### Toolbar actions

| Button | Action |
|--------|--------|
| **🔄 Backtest** | Re-run selected results as new backtests (opens date/balance/exchange modal) |
| **▶ Add to Run** | Create a live run from the selected config |
| **📈 Compare** | Add selected results to the comparison view |
| **🗑 Delete Selected** | Delete selected results from disk |

### Per-row actions

| Icon | Action |
|------|--------|
| 📊 | Open the result charts (equity curve, TWE, etc.) |
| 🗑 | Delete this single result |

### Result charts

Clicking a row opens a full-featured chart panel with:
- **Equity curve** (log scale toggle)
- **PnL** over time  
- **TWE** (total wallet exposure) chart  
- **Hedged PnL** if available  
- Full **analysis metrics** table  
- **Config JSON** viewer  

Use **📌 Pin** to keep the chart visible while browsing other results.
Use **📈 Compare** to overlay multiple results on one chart.

### Re-backtest modal

Available from the **🔄 Backtest** toolbar button. Options:

| Option | Description |
|--------|-------------|
| **start_date / end_date** | Override the date range for the re-run |
| **starting_balance** | Override the starting balance |
| **Exchange(s)** | Override which exchange(s) to use |
| **📂 Use PBGui Market Data** | When checked, sets `ohlcv_source_dir` to the PBGui-managed data path |

---

## Archive panel

Community and personal config archives stored as Git repositories.

### Archive list view

| Button | Action |
|--------|--------|
| **⬇ Pull All** | Pull the latest commits from all configured archives |
| **⬆ Git Push** | Push your personal archive changes to its remote |
| **+ Add Archive** | Configure a new archive (URL, local path) |
| **⚙ Setup** | Edit archive settings |
| **📋 Log** | Open the archive sync log in a floating panel |

Click an archive row to open it and browse its results.

### Archive results view

| Button | Action |
|--------|--------|
| **🏠 Archives** | Return to the archive list |
| **🔄 Backtest** | Re-run selected configs as new backtests → switches to Queue |
| **▶ Add to Run** | Create a live run |
| **📈 Compare** | Add to comparison view |
| **🗑 Delete Selected** | Remove selected archive results |

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

