# PBGUI Services Overview

The Services page shows and controls all PBGui background services in one place.

## Service overview

The page displays six service columns, each with:

- A toggle to start/stop the service
- A status indicator (✅ running / ❌ stopped)
- A **Show Details** button to open the service detail view

| Service | Purpose |
|---|---|
| **PBRun** | Starts/stops local Passivbot bot processes and manages dynamic coin filters |
| **PBRemote** | Syncs instances and commands between local and remote VPS servers via a cloud bucket |
| **PBMon** | Monitors running bots and sends Telegram alerts for unusual behavior |
| **PBStat** | Collects spot trade statistics for the legacy v6 single bot only |
| **PBData** | Fetches real-time market data (OHLCV, orders, positions) from exchanges |
| **PBCoinData** | Fetches CoinMarketCap data and builds exchange symbol mappings for dynamic filters |

## Toggling services

Click the toggle to start or stop a service. Changes take effect immediately — PBGui starts or stops the corresponding background process.

## Show Details

Each service has a **Show Details** button that opens a dedicated detail view with:

- Current service status
- Service-specific configuration options (where available)
- Integrated filtered log viewer

Use the back button (`:back:`) in the sidebar or the top-left navigation to return to the overview.

## Typical startup sequence

A healthy setup usually starts services in this order:

1. **PBCoinData** — builds symbol mappings (required for dynamic ignore/approve lists)
2. **PBRun** — starts bot processes (uses mappings from PBCoinData)
3. **PBData** — provides live market data
4. **PBStat** — collects spot trade statistics (v6 single bot only)
5. **PBRemote** — connects to remote VPS (if used)
6. **PBMon** — enables monitoring and alerts (if used)

## Troubleshooting

- A service shows ❌ but toggle is on: check the corresponding log under `data/logs/` for errors
- **PBRun** lists look stale: confirm **PBCoinData** built its mappings successfully first
- After config change: restart the affected service by toggling it off then on
