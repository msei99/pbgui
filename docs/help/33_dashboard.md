# Dashboards

After an approved PBGui AI dashboard creation or layout change, the open Dashboard page refreshes its list and reloads the affected dashboard immediately. The reload is generation-safe: a manual dashboard switch made while the refresh is in flight is never overwritten by the delayed AI completion.

The **Dashboards** page provides a fully customisable portfolio overview for your live Passivbot instances.
You can build multi-widget layouts, combine data from multiple users, and rearrange everything freely.

Dashboard APIs, renderer scripts and WebSockets stay on PBGui's own origin, preserving a server-configured mount prefix. An `api_base` query parameter cannot select another server. Editor/template messages are accepted only from their expected same-origin parent or iframe, and external sites cannot frame the pages.

---

## Viewing a dashboard

- If only one dashboard exists it opens automatically.
- If multiple dashboards exist, a **selectbox** appears — pick the dashboard you want.
- All dashboards are listed as quick-access buttons in the sidebar.
- Widgets refresh automatically in the background. Hit ↻ to force-reload immediately.
- In Edit Mode, live refreshes defer rebuilding a widget while one of its selects, date fields, number fields, or buttons is active. Open Period/User controls therefore stay open, and the latest pending data is applied once the control loses focus.
- Bitunix and WEEX PB8 users use PB8's own read-only clients for wallet balance, positions, orders, prices/OHLCV, and fill-derived realised trade PnL. The clients run in isolated short-lived processes and are closed after every request; a bounded snapshot avoids duplicate account calls.
- Bitunix funding income is not available from PB8 v8.1.0's connector, so its performance widgets currently cover fill-derived realised PnL and fees rather than claiming complete funding parity. Production acceptance still requires live read-only credentials for exchange response and pagination verification.
- Direct **Market close** remains visible but disabled for Bitunix and WEEX until each exchange's hedge/one-way and close-order contract has been separately live-tested. Panic, Graceful Stop, and Take Profit Only remain available because they update Passivbot configuration rather than guessing exchange order parameters.

---

## Sidebar actions

| Button | Action |
|--------|--------|
| ↻ | Reload the current dashboard |
| ➕ | Create a new empty dashboard |
| ✎ | Edit the currently displayed dashboard |
| 🗑 | Delete the current dashboard |
| 📋 | Open the Templates panel |

When **editing**, the sidebar switches to:

| Button | Action |
|--------|--------|
| 💾 | Save changes to disk |
| ✕ | Discard all changes and restore the previous state |
| 🗑 | Delete the current dashboard permanently |

---

## The editor

Click ✎ to open the dashboard editor in a dedicated full-screen tab.

### Layout — rows and columns

The toolbar at the top controls the grid structure:

- **Name** — the dashboard name shown in the sidebar.
- **1 COL / 2 COL** — toggle between a 1-column and 2-column grid.
  - **1 COL**: one wide cell per row — ideal for full-width charts.
  - **2 COL**: two cells side by side — e.g. Positions on the left, Orders on the right.
- **Rows** — add or remove rows with the `+` / `−` buttons. Each row holds 1 or 2 cells depending on the column setting.

### Assigning widgets to cells

Each cell shows a header bar with a **type badge** (e.g. `NONE`, `📊 PNL`, `📋 POSITIONS`, …).
Click the badge or select a type from the dropdown to assign a widget.
You can also **drag** a widget type from the **palette** on the right of the toolbar and drop it onto a cell.

### Drag & Drop — rearranging widgets

- **In the editor**: grab a cell by its dark header bar (the one containing the type badge and 🗑 icon) and drag it onto another cell — the two cells swap positions.
- **In the live view**: grab a widget by its coloured title bar (e.g. "Positions", "Orders") and drop it onto another widget to swap them. Swaps are saved to disk automatically.

### Resizing cells

Each cell has a **resize handle** at the bottom-right corner. Drag it to make the cell taller or shorter. The height is stored per cell in the dashboard.

Releasing outside the editor or leaving the browser window ends the resize and restores scrolling.

### Cell configuration panel

Each cell shows a compact configuration area directly below the header:

- **Users** — which account(s) to load data from. `ALL` means all accounts, or pick specific users.
- **Period / From / To / To now** — time range controls (available per widget type).
- **Link to Positions** (ORDERS only) — a chip picker to connect the Orders chart to a specific Positions widget in the same dashboard.
- **Mode** (PNL / ADG) — switch between chart styles (bar, line, …).

---

## Widget types

### ⚖️ BALANCE

Shows the **current USDT / USDC balance, open PnL, and total equity** for one or all accounts.
The user selector is embedded directly in the widget header — no separate config row.
Best placed as a compact summary cell at the very top of the dashboard.

### 📊 PNL — Daily PnL

A **bar chart** of realised PnL per calendar day.
Green bars = profitable day, red bars = loss day.
A thin cumulative line overlays the bars to show overall trend.

Configuration:
- **Mode** — `bar` (default) or `line`
- **Period** — preset ranges (`ALL_TIME`, `1_MONTH`, `3_MONTHS`, …) or a custom From / To date
- **Users** — filter by account

### 📈 ADG — Average Daily Gain

A **line chart** of cumulative USDT balance over time, showing the overall growth curve.
The ADG percentage (average daily gain) is displayed in the widget header.

Same period and users controls as PNL.

### 📉 P+L — Cumulative PnL per symbol

Plots **separate cumulative PnL lines** for each symbol in one chart.
Makes it easy to see which coins drive gains or losses over a time period.

### 💰 INCOME — Income by symbol

A **line chart** of cumulative income over time, with one line per symbol.
Useful for identifying the best and worst performing coins over the selected period.

In Table mode, income rows initially appear newest first. Clicking **Date** reverses the sort direction.

#### Restoring an income backup

The restore picker restores the complete local `pbgui.db` snapshot, not only the visible income rows. The separate trades database is unchanged.

- Backups include committed WAL data. Restore validates a private snapshot, SQLite integrity, and the PBGui schema before changing live data; linked files and incomplete WAL-dependent backups are rejected.
- Snapshot schema is checked before integrity expressions run. Triggers, views, virtual tables and unsupported expressions are rejected; SQLite parsing and allocation limits also reject oversized content. Restore is for compatible PBGui snapshots, not arbitrary SQLite files.
- Restore uses one SQLite transaction without replacing the live database file or deleting its WAL/SHM files. Existing readers can finish their current snapshot; subsequent reads see the restored data.
- PBData is not stopped by restore. Its incremental history scans, local DB Sync cycles and user imports cannot overlap restore admission. A busy database, active scan, local install or master operation can return HTTP 409; wait for the operation to finish before retrying.
- API restart is blocked while restore is active. Failed or timed-out unfinished restores roll back; temporary files and leases are released.
- Live collection resumes after restore and can immediately update current balances, prices, positions, and history. After installing this fix, reload both the API and PBData before using restore so all processes use the new coordination; older detached DB Sync workers must finish first.
- Income deletion is refused if its automatic safety backup cannot be created.

Extra controls:
- **Last N** — show only the top-N symbols by absolute value
- **Filter** — hide symbols below a minimum absolute income threshold

### 🏆 TOP — Top symbols

Ranks all symbols by total income and displays the top N as a horizontal bar chart.
Negative earners appear in red. Good for a quick performance ranking at a glance.

Configuration:
- **Top N** — how many symbols to include
- **Period / From / To** — time range

### 📋 POSITIONS

A **live table** of all currently open positions across the selected accounts.

Columns: User · Symbol · Side · Size · uPnL · Entry · Price · DCA · Next DCA · Next TP · Pos Value

- Rows update automatically as new data arrives from the exchange.
- **Click a row** to select that position — the linked **📝 ORDERS** widget immediately loads the price chart for that symbol with order markers.
- The Users dropdown in the widget header filters by account.

#### Manage positions

The **Manage** button next to the Positions title opens a draggable, resizable dialog for the currently visible positions.

Per-row actions:
- **Market close amount** sends a direct reduce-only market order for the selected row amount.
- **Panic symbol** saves a runtime-aware per-symbol forced-mode override and syncs the config.
- **Graceful stop symbol** saves a runtime-aware per-symbol `graceful_stop` forced-mode override and syncs the config.
- **Take profit only symbol** saves a runtime-aware per-symbol `tp_only` forced-mode override and syncs the config.

All-position actions at the bottom of the dialog:
- **Preview Panic**, **Preview Graceful stop**, **Preview TP only** show the config that would be saved without writing or syncing anything.
- **Panic**, **Graceful stop**, **Take Profit Only** save the corresponding global PB7 or PB8 forced mode for long and short positions. PB7 is pushed and materialized immediately on its assigned Cluster bot host. PB8 uses its validated bundle save and publishes an exact Cluster operation for normal PB8 reconciliation. PB8 symbol actions update the referenced sparse override JSON instead of incorrectly embedding V7-style override fields in the main config.

The Exchange user must resolve to exactly one local PB7 or PB8 instance. If the same user is assigned to multiple instances or runtimes, PBGui refuses the config action rather than guessing which bot owns the position.

Panic, Graceful Stop and Take Profit Only are Passivbot config/sync actions. They do not submit direct exchange orders. Use **Market close amount** when you explicitly want PBGui to send a direct reduce-only market order.

### 📝 ORDERS

A **candlestick chart** with overlaid order markers (entries, take-profits, DCA orders) for the position selected in the linked Positions widget.

Setup:
1. Add both a 📋 POSITIONS cell and a 📝 ORDERS cell (2-column layout works best).
2. In the ORDERS cell config, click the **Link to Positions** chip matching the Positions cell (e.g. "Row 3 · Col 1").
3. In the live view, click any position row — the chart loads automatically.

Controls:
- **Timeframe buttons** (1m 5m 15m 30m 1h 2h 4h 6h 12h 1d 1w) — change candle resolution.
- **Scroll left** on the chart to load older historical candles.

---

## Templates

Templates are **pre-built dashboard layouts** you can use as a starting point.
Instead of building a grid from scratch, apply a template to instantly fill the cells with a sensible widget arrangement.

After creating dashboards from a template, the panel closes and the last successfully created dashboard opens. Failed or skipped creations are not selected.

### Applying a template

1. In the sidebar, click 📋 **Templates**.
2. A panel opens showing all available templates with small previews.
3. Click a template to apply it — the current grid is replaced with the template layout.
4. Adjust the users, periods, and configuration in each cell to match your setup.
5. Click 💾 to save the result.

> **Note:** Applying a template overwrites the current grid. Save any changes you want to keep before applying.

### Available templates

| Template | Layout | Contents |
|----------|--------|----------|
| **Overview 2×3** | 2 cols, 3 rows | ⚖️ Balance, 💰 Income, 📊 PNL, 📈 ADG, 📋 Positions, 📝 Orders |
| **Single user** | 1 col, 4 rows | ⚖️ Balance, 📈 ADG, 📊 PNL, 📋 Positions stacked vertically |
| **Positions & Orders** | 2 cols, 1 row | 📋 Positions left, 📝 Orders right — auto-linked |

---

## Creating a dashboard

1. Click ➕ in the sidebar.
2. Enter a **name** in the dialog and click **Create & Edit**.
3. Choose **1 or 2 columns** and the number of **rows**.
4. Assign a widget type to each cell (drag from palette or click the type badge).
5. Configure each widget (users, time period, links between cells).
6. Click 💾 — the dashboard is saved and immediately shown in the live view.

Cancelling an unsaved new dashboard returns to an idle content area without a loading spinner.

---

## Deleting a dashboard

1. Open the dashboard in the editor (✎).
2. Click 🗑 in the sidebar.
3. Confirm the deletion. The dashboard is permanently removed.

---

## Tips

- Use a **2-column layout** and place ⚖️ **BALANCE** spanning both cells of the top row for an instant equity snapshot.
- In a 2-column layout, put 📋 **POSITIONS** and 📝 **ORDERS** side by side so clicking a position opens its chart directly in the adjacent widget.
- Use 📈 **ADG** and 📊 **PNL** together to compare long-term growth with day-to-day fluctuations.
- In the live view, rearrange widgets by dragging their title bars — no need to open the editor.
- Dashboards with many cells using `ALL` users may load slower — assign specific users for better performance.
