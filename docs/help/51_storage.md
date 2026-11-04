# Storage

**System -> Storage** shows how much disk space PBGui, PB7, PB8 and their Python environments use, what can be cleaned up, how usage develops over time and the main system values of this host.

## Background scan

The folder sizes come from a background scan. It walks the PBGui folder, the configured PB7 and PB8 folders and the PB/PBGui virtual environments next to PBGui. The scan:

- runs in its own low-priority process, so the API and running bots are not slowed down
- repeats automatically at the configured interval (default 6 hours, see **Retention**)
- also starts when you open the page and the last scan is older than one hour
- is not needed after a cleanup: deleted entries are removed from the result immediately

The header shows when the last scan finished. Disk usage per filesystem is always live.

## Overview

- **Disk**: used and free space per filesystem, plus a bar that splits the used space into PBGui, PB7, PB8, Python environments and everything else on the disk.
- **Reclaimable**: totals per tier with the default filters. **Preview safe cleanup** and **Preview optional cleanup** collect every category of that tier using your current filters and open a preview. Each tile lists its categories with their size; click a category to open its entries.
- **Largest folders**: an expandable tree of the biggest folders, up to four levels deep.
- After a cleanup, the result (deleted entries, freed space, errors) is shown below the tiers.

## Tiers

| Tier | Meaning |
| --- | --- |
| **Safe** | No useful data is lost. Rotated logs, old job logs, old run logs, database backups beyond the newest ones and leftovers of finished repairs. Can be cleaned automatically. |
| **Optional** | Recreated when needed or of low value, such as PB7's prepared backtest datasets or cancelled and failed cloud jobs. Never selected unless you choose it. |
| **Review** | Your own results and possible leftovers: PB7/PB8 optimize results and backtests, completed cloud jobs, PB7 legacy data, release worktrees, manual copies and unused Python environments. You select each entry yourself. |
| **Protected** | Market data, the database, credentials, API keys and configurations. Shown for information only; Storage never deletes them. |

## Category views

**Logs**, **Jobs & Results**, **Caches & Market Data** and **Backups & Leftovers** list their categories with entry count, size, oldest entry and the entries matching the filter.

- **Older than (days)** only matches entries whose newest file is older than this age. For folders the newest file inside the folder counts.
- **Keep newest** always keeps this number of the newest entries (database backups).
- Filters are remembered in this browser.
- **Show entries** / **Choose entries** opens the entry list. Click a row to select it, drag over rows or Shift-click to select a range, and use Space or Enter on the keyboard. **Select matching** selects every entry that matches the filter.
- Greyed-out entries are in use and cannot be selected, for example logs of running jobs, folders changed within the last 24 hours, Python environments used by PBGui, PB7, PB8, a running process or a systemd service, and cloud jobs that are still active.

### Notes on single categories

- **PB7 legacy historical data**: PB7 copied `historical_data/` into `caches/ohlcv/` and leaves the old folder for manual deletion. The category is only available after PB7 has marked the migration as finished (`caches/ohlcv/.migration_done`).
- **PB7 prepared backtest data** is blocked while a PB7 backtest or optimize is running.
- **Cloud jobs** are deleted through the Vast queue's own purge, together with their retry attempts. Lineages with a completed attempt are listed under **Completed cloud jobs**.
- **Release worktrees** are removed with `git worktree remove`.
- **Market data** (PBGui archive, PB7 OHLCV cache and store) is managed in **Information -> Market Data**.

## Preview and deletion

Every cleanup starts with a preview that lists the entries, their sizes and the total. All entries start selected; click a row to leave it out, drag over rows or Shift-click for a range, or use **Select all** / **Select none**. **Delete** removes only the selected entries and asks for a final confirmation. Right before deleting each entry, the worker checks it on disk again with the current time and skips entries that are now in use or no longer match, for example a results folder that received new files during the cleanup. Folders are first moved aside and then deleted; if PBGui stops in between, the deletion finishes on the next run. **Stop** in the progress bar ends the cleanup after the current file.

Only one scan or cleanup runs at a time. A preview expires after 30 minutes.

## Retention

- **Scan interval (hours)**: how often the background scan runs.
- **Automatic cleanup**: runs the enabled rules once per day after the selected hour. Only categories listed in the rules table can be cleaned automatically. Each enabled rule needs an age, a number of entries to keep, or both.
- Automatic cleanup is off by default. Its result appears on the Overview like a manual cleanup.

## History

One data point per day is kept (up to about two years): used disk space and the size of PBGui, PB7, PB8, the Python environments and their largest folders. The table shows the growth over the last 7 and 30 days.

## System

CPU model, cores, usage and load, memory, swap, uptime, operating system, the Python versions of PBGui, PB7 and PB8, NVIDIA GPUs (when `nvidia-smi` is available), disk usage and the list of PBGui processes with memory and CPU usage. The values update every few seconds while the view is open.
