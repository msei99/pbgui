# PBAPIServer Service

PBAPIServer is the FastAPI backend that powers all real-time features of PBGui. It provides REST endpoints, WebSocket streams, and serves the frontend pages (Dashboard, Services, VPS Monitor, etc.).

## What PBAPIServer does

- Runs the FastAPI server (default port 8000) with REST, WebSocket, and SSE endpoints
- Powers the Dashboard with a 3-tier data architecture:
  - **Layer 1 (background):** PBData polls REST APIs and writes to the database; notifies the API server via internal localhost endpoints
  - **Layer 2 (on-demand):** `api/live.py` opens private ccxtpro WebSocket connections to exchanges (for positions/balances) when a browser subscribes — ref-counted and shut down when no browsers are connected
  - **Layer 3 (browser):** Vanilla JS receives updates via SSE (Server-Sent Events)
- Powers the Services page (start/stop/restart all PBGui daemons)
- Powers the VPS Monitor (SSH connections, live metrics, remote log streaming, file sync)
- Manages the Job Queue (backtests, optimizations) with real-time status updates
- Serves API Keys management endpoints
- Serves Market Data pipeline status and control
- Provides live log streaming from `data/logs/` via WebSocket
- Hosts the Heatmap data endpoints
- Serves all Vanilla JS frontend pages from the `frontend/` directory

## Configuration

PBAPIServer settings are stored in `pbgui.ini` under `[api_server]`:

| Setting | Default | Description |
|---|---|---|
| `host` | `0.0.0.0` | Bind address (`0.0.0.0` = all interfaces, `127.0.0.1` = localhost only) |
| `port` | `8000` | API server port (1024–65535) |

You can change host and port on the **PBAPIServer Details** page (`System → Services → PBAPIServer → Settings` tab).

## Starting and stopping

- **Start**: Use the Start button on the Services overview or details page. PBAPIServer spawns as a background process.
- **Stop**: Not supported from GUI (the server cannot stop itself while serving the page). Stop via terminal if needed.
- **Restart**: Use the Restart button. The server gracefully shuts down and respawns after a short delay (3 seconds) to ensure the port is released.

The nav bar shows an orange **Restart** button when the API code has changed (detected via `api/serial.txt`). Clicking it triggers a graceful restart and page reload.

## WebSocket endpoints

PBAPIServer provides several real-time WebSocket streams:

| Endpoint | Server message format | Client input |
|---|---|---|
| `/ws/jobs` | `{"type":"jobs","data":[...],"timestamp":...}` with up to 50 pending/running jobs | None |
| `/ws/dashboard` | `balance_updated`, `income_updated`, `positions_updated`, `nav_request`, or `dashboard_action` envelopes | None |
| `/ws/candles` | `candle`, `position`, `orders`, or `ping` envelopes | Query: `user`, `symbol`, optional `tf`, `side` |
| `/ws/market-data` | Flat `market_data_status` envelope with exchange, running/queued state, counters and `coin_rows` | Query: `exchange` |
| `/ws/vps` | `state`, `log_lines`, `local_log_lines`, command results, or `error` | JSON commands with `cmd` |
| `/ws/heatmap-watch` | `{"type":"updated","mtime":...}` | Query: `exchange`, `dataset`, `coin` |
| `/api/v7/ws/v7` | `{"type":"instances","data":[...]}` | Received text is ignored |
| `/api/backtest-v7/ws/bt7` | `queue_update` or `archive_update` | `{"type":"refresh"}` |
| `/api/optimize-v7/ws/opt7` | `queue_update` | `{"type":"refresh"}` |
| `/api/vps-manager/ws` | `state`, `detail`, `result`, `error`, and command-specific envelopes | JSON commands with `cmd` |

Browser WebSocket connections authenticate through the HttpOnly `pbgui_session` cookie. Invalid or revoked sessions are closed with code `4001`.

## Authentication

Browser pages and WebSockets use the HttpOnly `pbgui_session` cookie. API clients may continue to use `Authorization: Bearer xxx` for REST requests.

Tokens are generated at login and expire after 24 hours. All FastAPI pages automatically refresh tokens every 30 minutes. If a token expires, the page redirects to the login screen.

## Logs

PBAPIServer writes to `data/logs/PBApiServer.log`. Log entries include:
- Server startup and shutdown events
- HTTP request logging (from uvicorn)
- WebSocket connection events
- Serial file change detection (`[serial-watcher]`)
- Task worker watchdog events (`[watchdog]`)

## Background watchers

PBAPIServer runs several internal background tasks:

- **Task-worker watchdog**: Checks every 60 seconds if the job queue worker is alive; auto-restarts it if crashed
- **Serial watcher**: Monitors `api/serial.txt` via inotify for changes; broadcasts a restart notification to all connected clients via SSE
- **VPS Monitor**: Manages SSH connection pool, live metrics, and remote log streaming for connected VPS hosts
- **File Sync Worker**: Watches local config files and syncs changes to remote VPS hosts via inotifywait

## Troubleshooting

| Symptom | Check |
|---|---|
| Server won't start | Check if port is already in use (`lsof -i :8000`); check `data/pid/api_server.pid` for stale PID |
| "Address already in use" | Previous server didn't shut down cleanly — wait a few seconds or kill the old process |
| Orange Restart button won't go away | Click it to restart; `api/serial.txt` was incremented after a code change |
| WebSocket disconnects | Check `PBApiServer.log` for `[ERROR]` lines; verify token is still valid |
| Dashboard not loading | Confirm PBAPIServer is running; check browser console for connection errors |
