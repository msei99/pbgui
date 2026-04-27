# Logging

The Logging page provides a real-time log viewer for all PBGui services.  
Logs stream live via WebSocket — no page reload required.

## Layout

- **Sidebar (left)**: list of all available log files in `data/logs/`, with adjustable width on desktop
- **Toolbar**: level filter, lines count, version selector, stream controls
- **Log area**: scrollable terminal output with search and highlighting

## Selecting a log file

Click any file name in the sidebar to load it.  
The current file name and size appear in the toolbar.  
Streaming starts automatically — new lines are appended in real time.

## Lines dropdown

Controls how many lines are loaded when opening or switching a file.  
Options: 200 / 500 / 1000 / 2000 / 5000 / 10000 / 25000 / All.  
Changing the value while a file is open reloads with the new count.

## Version dropdown

Appears when the selected log file has rotated backups (`.1`, `.old`, etc.).  
- **Current**: live file with streaming
- **.1 / .old / …**: archived snapshot, loaded once (no streaming)

Switching back to **Current** resumes live streaming.

## Level filter

Toggle individual log levels on/off:

| Button | Level    |
|--------|----------|
| DBG    | DEBUG    |
| INF    | INFO     |
| WRN    | WARNING  |
| ERR    | ERROR    |
| CRT    | CRITICAL |

Lines not matching an active level are hidden instantly without refetching.

## Search

- Type in the **Search** field to filter or highlight matching lines
- **Filter** checkbox: when checked, hides non-matching lines; when unchecked, highlights only
- Use the **▲ / ▼** buttons to jump between matches
- **Preset** dropdown: common search patterns (Errors, Warnings, Traceback, …)

## Stream controls

| Button    | Action                                         |
|-----------|------------------------------------------------|
| ⏸ Pause  | Stop receiving new lines (buffer is kept)      |
| ▶ Stream | Resume live streaming from current position    |
| 🗑 Clear  | Clear the display buffer (does not delete file) |
| ⬇ Download | Save current buffer content as a text file   |
| ## Lines | Toggle line number display                      |

## Settings

Click **⚙ Settings** in the sidebar to configure log rotation:

- **Default rotation**: max file size (MB) and number of backup files for all services
- **Per-log rotation**: override size and backup count per individual service log

Changes take effect when the service logger is next initialized (restart service if needed).

## Troubleshooting

- **No log files listed**: make sure PBGui services have been started at least once
- **Streaming stops**: PBAPIServer WebSocket connection lost — the viewer reconnects automatically
- **Lines count "All" is slow**: loading very large files may take a moment; use a line limit for large logs

---

## Where to find what

All log files are located in `data/logs/`. Use the sidebar to open any of them directly.

### PBGui.log

Contains messages from all GUI helper components:

| Component | What you find there |
|-----------|-------------------|
| VPSManager | VPS connections, remote command results |
| Instance | Bot instance load/save, symbol info |
| Config | Config load/save errors |
| Multi | Multi-bot config operations |
| Backtest / BacktestV7 | Backtest result loading, corrupted files |
| BacktestMulti | Multi-symbol backtest operations |
| Optimize / OptimizeV7 / OptimizeMulti | Optimizer operations |
| ParetoDataLoader | Pareto result loading |
| Status | Status page events |
| HyperliquidAWS | Hyperliquid AWS integration |

### Dedicated log files

| File | Service | What you find there |
|------|---------|-------------------|
| `PBRun.log` | PBRun | Live bot start/stop, order loop |
| `PBRemote.log` | PBRemote | Remote sync, VPS communication |
| `PBCoinData.log` | PBCoinData | CMC data updates, symbol lists |
| `VPSMonitor.log` | VPS Monitor | SSH connections, host metrics, service auto-heal |
| `PBApiServer.log` | PBAPIServer | FastAPI startup, REST/WebSocket requests |
| `PBStat.log` | PBStat | Statistics collection |
| `Database.log` | Database | DB queries, connection errors |
| `Exchange.log` | Exchange | Market fetch, symbol info, CCXT errors |
| `PBData.log` | PBData | OHLCV download, market data pipeline |
