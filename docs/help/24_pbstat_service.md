# PBStat Service Details

PBStat is a legacy background service that collects live trade statistics (PnL, fills) for the old **v6 single bot** spot trading instances.

> **Note:** This service is only relevant if you are still running legacy v6 spot bots. It is not used for v7 perpetual futures instances.

## What PBStat does

- Connects to active v6 single bot instances
- Collects and aggregates trade statistics (PnL, fills)
- Writes service logs to `data/logs/PBStat.log`

## PBStat Details page

On the `System → Services → PBStat → Show Details` page you can:

- Check current PBStat service status (running/stopped)
- Toggle the service on/off
- Use the integrated filtered PBStat log viewer

## Troubleshooting

- If you are only running v7 instances, you can safely leave PBStat stopped.
