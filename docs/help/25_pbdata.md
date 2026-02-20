# PBData (Service)

PBData is a background service inside PBGui. It continuously fetches account data (via WS + REST) and writes it into PBGui’s database so other pages can load it quickly.

## What PBData fetches

Per selected user (see **System → Services → PBData Details**):

- **WebSocket (private)**
  - balances
  - positions
  - orders
- **Shared REST pollers** (serialized “round-robin” style)
  - combined poller: balances/positions/orders (fallback + periodic refresh)
  - history poller
  - executions poller (my trades) — *opt-in*

## Users vs. Executions download (opt-in)

PBData has two separate user lists:

- **Users**
  - Users PBData actively updates (WS + REST)
- **Executions download**
  - **Opt-in allow-list**: only these users download/store executions (my trades)
  - Default is **none**
  - Changing this list takes effect quickly; PBData re-checks it before each executions fetch

## Timers and performance

In **PBData timers** you can tune how aggressively PBData polls.

- **Startup delay (s)**
  - Grace period after PBData starts before shared REST pollers begin
- **Combined interval (s)**
  - How often to run the shared combined REST poll (balances/positions/orders)
- **History interval (s)**
  - How often to run shared history updates
- **Executions interval (s)**
  - How often to run shared executions (my trades)

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

## Private WS global limit (ws_max)

- **Max private WS global** caps how many private websocket clients PBData may keep open.
- If you run many users/exchanges, this helps prevent websocket overload.

## Log viewer tips

The PBData details page uses the filtered log viewer for PBData logs. It supports:

- Selecting one or more **Logfiles** (merged by timestamp)
- Filtering by:
  - **Users**
  - **Tags** (from `[tag]` tokens)
  - **Levels (filter)**
  - **Free-text**
- **RAW** mode shows unformatted lines
- Buttons:
  - ✖ Clear filters
  - 🔄 Refresh
  - 🗑️ Purge/truncate the selected log file(s)

PBData also has a separate **PBData Log level** selector in the log header, which controls how verbose PBData itself logs.

## Fetch Summary panel

PBData Details also shows a **Fetch Summary** panel (from `data/logs/fetch_summary.json`).

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

- Click 🔄 in the log viewer
- Click 🔄 in the Fetch Summary panel
- Check whether PBData is running (PBData toggle)
- Consider increasing the combined poll interval if the system is overloaded
