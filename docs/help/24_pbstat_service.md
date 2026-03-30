# PBStat Service Details

PBStat is a legacy background service that collects live trade statistics (PnL, fills) for the old **v6 single bot** spot trading instances.

> **Note:** This service is only relevant if you are still running legacy v6 spot bots. It is not used for v7 perpetual futures instances.

## What PBStat does

PBStat runs a 60-second daemon loop. Every 5th cycle it performs a full fetch, other cycles do a lighter status-only check.

- Fetches position, balance, price, and open orders from the exchange for each active spot instance
- Fetches trade history since the last known trade and appends new trades to the instance's `trades.json`
- After each cycle, reloads all instances from disk to pick up added/removed instances
- Writes service logs to `data/logs/PBStat.log`

## PBStat detail panel

Click the PBStat card on the Services overview (or use the sidebar) to open the detail panel:

- The control strip shows the current status (running/stopped) and Start/Stop/Restart buttons
- The Log tab shows a live filtered PBStat log viewer

## Troubleshooting

- If you are only running v7 instances, you can safely leave PBStat stopped.
