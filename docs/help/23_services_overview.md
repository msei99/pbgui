# PBGUI Services Overview

The Services page shows and controls all PBGui background services in one place.

## Service overview

The page opens with a card grid showing all services at a glance. Each card displays:

- The service name
- A status indicator (green dot = running, red dot = stopped)
- Action buttons: **Start** when stopped, **Stop + Restart** when running

Click a card to open that service's detail panel.

The Overview also includes a dedicated **Workers** card. It opens the admin-only worker area for queue workers, sync/watcher workers, and internal helper tasks.

| Service | Purpose |
|---|---|
| **PBCluster** | Replicates Cluster Sync state and materializes approved V7/API-key changes to joined nodes |
| **PBRun** | Starts/stops local Passivbot bot processes and manages dynamic coin filters |
| **PBStat** | Collects spot trade statistics for the legacy v6 single bot only |
| **PBData** | Fetches account data (balances, positions, orders, history, executions) via REST and live prices via public WebSocket |
| **PBCoinData** | Fetches CoinMarketCap data and builds exchange symbol mappings for dynamic filters |
| **PBAPIServer** | Runs the FastAPI backend (REST + WebSocket) that powers the Dashboard, VPS Monitor, Job Queue, live alert handling, and all real-time features |

## Starting and stopping services

Use the **Start**, **Stop**, or **Restart** buttons on each card or in the control strip at the top of a service's detail panel. Changes take effect immediately.

## Service detail panels

Click a service card (or its sidebar entry) to open a dedicated detail panel with:

- A control strip showing the service status and action buttons
- Tabs for different views (where available):
  - **Log**: Live filtered log viewer
  - **Settings**: Service-specific configuration
  - **Status**: Runtime status (PBData only)

Use the sidebar on the left to switch between services or return to the Overview.

## Workers panel

The **Workers** sidebar entry opens a dedicated admin panel inside the Services page. It is intended for operations and troubleshooting, not for day-to-day bot usage.

The panel groups workers into:

- **Queue Workers**: shared Market Data queue worker, Backtest queue worker, Optimize queue worker
- **Sync / Watchers**: API key file sync watcher, V7 config sync watcher
- **Internal Helpers**: archive sync and HLCVS cleanup background tasks

For each worker you can inspect:

- Running/stopped state
- Small runtime statistics such as queued items, active jobs, connected hosts, or watchdog state
- Start/Stop/Restart actions where supported
- A local log viewer when the worker writes its own log file

Stop and Restart actions in the Workers panel ask for confirmation before the command is sent.

Some workers expose a monitor instead of a dedicated local log. For example, the shared Market Data queue worker uses the Job Monitor because job logs are tracked per queued job. In those cases selecting the worker embeds the monitor directly in the right-hand log pane, keeps it in place during worker refreshes, and lets you stay inside the Services page. The embedded Job Monitor now exposes `View` for full job details and `Run` on pending rows; `Run` requests one extra manual same-type parallel slot, so one selected pending job can start alongside the already running job of that type. Active rows also keep a stable queue/start order now, so live progress updates no longer reshuffle two running jobs against each other. `View` and `Log` dialogs are clamped to the visible browser viewport as well, and they now track both the outer page scroll offset and clipping parent panels, so their close button stays reachable even when the monitor sits inside a taller embedded region whose header is already above the visible browser window.

## Typical startup sequence

A healthy setup usually starts services in this order:

1. **PBCoinData** — builds symbol mappings (required for dynamic ignore/approve lists)
2. **PBRun** — starts bot processes (uses mappings from PBCoinData)
3. **PBData** — provides live market data for the Dashboard
4. **PBStat** — collects spot trade statistics (v6 single bot only)
5. **PBCluster** — handles Cluster Sync for joined nodes when cluster mode is enabled
6. **PBAPIServer** — enables Dashboard, VPS Monitor, Job Queue, and real-time features
7. **PBAPIServer VPS Monitoring Alerts** — configure Telegram routing and in-GUI alert visibility inside the API server settings when needed

## Troubleshooting

- A service shows a red dot but should be running: check the corresponding log in the service's Log tab for errors
- **PBRun** lists look stale: confirm **PBCoinData** built its mappings successfully first
- After config change: restart the affected service via the Restart button
