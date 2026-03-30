# PBGUI Services Overview

The Services page shows and controls all PBGui background services in one place.

## Service overview

The page opens with a card grid showing all services at a glance. Each card displays:

- The service name
- A status indicator (green dot = running, red dot = stopped)
- Action buttons: **Start** when stopped, **Stop + Restart** when running

Click a card to open that service's detail panel.

| Service | Purpose |
|---|---|
| **PBRun** | Starts/stops local Passivbot bot processes and manages dynamic coin filters |
| **PBRemote** | Syncs instances and commands between local and remote VPS servers via a cloud bucket |
| **PBMon** | Monitors running bots and sends Telegram alerts for unusual behavior |
| **PBStat** | Collects spot trade statistics for the legacy v6 single bot only |
| **PBData** | Fetches account data (balances, positions, orders, history, executions) via REST and live prices via public WebSocket |
| **PBCoinData** | Fetches CoinMarketCap data and builds exchange symbol mappings for dynamic filters |
| **PBAPIServer** | Runs the FastAPI backend (REST + WebSocket) that powers the Dashboard, VPS Monitor, Job Queue, and all real-time features |

## Starting and stopping services

Use the **Start**, **Stop**, or **Restart** buttons on each card or in the control strip at the top of a service's detail panel. Changes take effect immediately.

## Service detail panels

Click a service card (or its sidebar entry) to open a dedicated detail panel with:

- A control strip showing the service status and action buttons
- Tabs for different views (where available):
  - **Log**: Live filtered log viewer
  - **Settings**: Service-specific configuration
  - **Status**: Runtime status (PBData only)
  - **Info**: Remote server info (PBRemote only)

Use the sidebar on the left to switch between services or return to the Overview.

## Typical startup sequence

A healthy setup usually starts services in this order:

1. **PBCoinData** — builds symbol mappings (required for dynamic ignore/approve lists)
2. **PBRun** — starts bot processes (uses mappings from PBCoinData)
3. **PBData** — provides live market data for the Dashboard
4. **PBStat** — collects spot trade statistics (v6 single bot only)
5. **PBAPIServer** — enables Dashboard, VPS Monitor, Job Queue, and real-time features
6. **PBRemote** — connects to remote VPS (if used)
7. **PBMon** — enables monitoring and Telegram alerts (if used)

## Troubleshooting

- A service shows a red dot but should be running: check the corresponding log in the service's Log tab for errors
- **PBRun** lists look stale: confirm **PBCoinData** built its mappings successfully first
- After config change: restart the affected service via the Restart button
