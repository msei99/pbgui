# PBData (Service)

PBData is a background service inside PBGui. It continuously fetches account data via REST and live prices via public WebSocket, then writes everything into PBGui's database so other pages can load it quickly.
Click the PBData card on the Services overview to open the detail panel with three tabs: **Log**, **Settings**, and **Status**.
## What PBData fetches

### Prices (public WebSocket)

PBData opens one **public** WebSocket per exchange and subscribes to price tickers for all symbols that have open positions. Prices are buffered in memory and flushed to the database every 10 seconds.

### Account data (REST pollers)

All account data is fetched via REST — PBData does **not** use private WebSocket connections. Per selected user (see **Settings** tab → Users):

- **Combined poller** (one per exchange, serialized per user)
  - balances (default every ~300 s)
  - positions (default every ~300 s)
  - orders (default every ~60 s)
- **History poller** (one per exchange)
  - income / funding history
- **Executions poller** (single task)
  - my trades — *opt-in*, only for users in the Executions download list

### Latest 1-minute candles

Separate tasks fetch the latest 1-minute OHLCV candles for Hyperliquid, Binance, and Bybit (used by the market data pipeline).

## Users vs. Executions download (opt-in)

PBData has two separate user lists:

- **Users**
  - Users PBData actively fetches via REST
- **Executions download**
  - **Opt-in allow-list**: only these users download/store executions (my trades)
  - Default is **none**
  - Changing this list takes effect quickly; PBData re-checks it before each executions fetch

## Timers and performance

In the **Settings** tab under **Timers** you can tune how aggressively PBData polls.

- **Max private WS**
  - Global cap on how many private WebSocket clients the Dashboard's live-streaming layer (`api/live.py`) may open. This does not affect PBData itself (which uses only REST), but the setting is managed here because PBData owns the exchange connection pool.
- **Startup delay (s)**
  - Grace period after PBData starts before shared REST pollers begin
- **Combined interval (s)**
  - How often the shared combined REST poll runs (balances + positions + orders fallback/refresh)
- **Balance interval (s)**
  - How often the dedicated balance REST poll runs
- **Positions interval (s)**
  - How often the dedicated positions REST poll runs
- **Orders interval (s)**
  - How often the dedicated orders REST poll runs
- **History interval (s)**
  - How often shared history updates run
- **Executions interval (s)**
  - How often shared executions (my trades) run
- **Market data coin pause (s)**
  - Pause between coin fetches in the 1-minute market data pipeline

General guidance:

- Too-small intervals can trigger **rate limits (HTTP 429)**.
- Increase intervals or reduce the number of active users if you see frequent backoffs.

## Rate-limit controls (REST pause)

PBData uses a small pause between users in shared REST pollers.

- **REST pause/user (s)**
  - Global pause between users during shared REST polling

### Shared REST pause per exchange

Some exchanges need a higher pause.

- You can set a per-exchange pause.
- If you leave a value equal to the global pause, PBGui will not save an override.
- If no override is set, PBData uses its built-in per-exchange defaults (example: Hyperliquid/Bybit).

## Log viewer tips

The PBData **Log** tab uses the live log viewer. It streams log lines via WebSocket and supports:

- **Files sidebar** — click the **Files** button (or the filename badge in the toolbar) to open a sidebar listing all available log files. Click a file to switch to it. Only one file is shown at a time.
- **Level filter buttons** — toggle **DBG**, **INF**, **WRN**, **ERR**, **CRT** to show/hide lines by severity
- **Search** — type free text into the search box, or pick a **Preset** (Errors, Warnings, Connection, Restart/Stop, Traceback). Toggle the **Filter** checkbox to switch between filtering (hide non-matches) and highlighting (show all, highlight matches). Use the **▲ / ▼** buttons to jump between matches.
- **Lines** — choose how many lines to keep in view (200 / 500 / 1000 / 2000 / 5000)
- **Control buttons**:
  - ⏸ **Pause** / ▶ **Resume** — freeze or resume the live stream
  - 🗑 **Clear** — clear all lines from the display
  - ↓ **Download** — download the currently viewed log
  - **# Lines** — toggle line numbers on/off

The **Log Level** setting (which controls how verbose PBData itself logs) is in the **Settings** tab, not in the log viewer.

## Status tab

The **Status** tab shows the Fetch Summary and Poller Metrics panels.

It gives a compact runtime snapshot of:

- balances / positions / orders fetch results
- history / executions results
- per-user last fetch timestamps and status

If no summary is visible yet, PBData likely has not written the first summary cycle.

## Where settings are stored

Most PBData settings are persisted in `pbgui.ini` under `[pbdata]`, including:

- `trades_users`
- polling intervals (`poll_interval_*_seconds`)
- `shared_rest_user_pause_seconds`
- per-exchange overrides (`shared_rest_pause_by_exchange_json`)
- `ws_max`
- `log_level`

## Troubleshooting

### I see many 429 / rate limit warnings

- Increase **REST pause/user**
- Increase poll intervals
- Reduce the number of active **Users**
- Consider using per-exchange pauses for sensitive exchanges

### Executions are not downloaded

- Ensure the user is selected in **Executions download**
- Check PBData logs for skipped/filtered executions messages

### The UI shows stale data

- Check whether PBData is running (Start/Stop buttons in the control strip)
- Open the **Status** tab and check the Fetch Summary for recent timestamps
- Consider increasing the combined poll interval if the system is overloaded
