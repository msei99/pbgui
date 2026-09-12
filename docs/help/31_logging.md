# Logging

Switching between Log Viewer and Settings preserves edited rotation values. Failed rotation saves show an error next to the affected settings.

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
Options: 200 / 500 / 1000 / 2000 / 5000 / 10000 / 25000 / Max (50,000).
Changing the value while a file is open reloads with the new count.
The selected count is the source-tail and rendered-line limit. Unfiltered and
remote views keep that bounded live buffer in the browser. An active local
filter keeps only matches and context in the browser while the server searches
the complete selected tail. **Max (50,000)** is a bounded tail, not the complete
file.

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

Local filtered searches are debounced and run on the server. Existing results
remain visible until the current request arrives, and live streaming sends only
new matches and required context. Clearing the filter restores the unfiltered
buffer. Download still saves the complete selected source tail.

## Stream controls

| Button    | Action                                         |
|-----------|------------------------------------------------|
| ⏸ Pause  | Stop receiving new lines (buffer is kept)      |
| ▶ Stream | Resume live streaming from current position    |
| 🗑 Clear  | Clear the display buffer (does not delete file) |
| ⬇ Download | Save the complete selected source tail       |
| ## Lines | Toggle line number display                      |

## Settings

Click **⚙ Settings** in the sidebar to configure log rotation:

- **Default rotation**: max file size (MB) and number of backup files for all services
- **Per-log rotation**: override size and backup count per individual service log
- **Managed logs**: configure size and backup count for dynamic log families
  such as API console, jobs, backtests, optimizations, VPS Manager runs,
  OHLCV preloads, monitor-agent live data, Pareto sessions, and API handoff

Changes are read on the next log write or before the next managed transcript is
opened. Logging rotation deliberately has no watcher and does not require a
service restart.

**Purge** force-rotates and empties the selected current log under the same
cross-process lock used by writers. PBGui keeps only the configured number of
numeric backup generations and stores at most the configured maximum-size tail
as `.1`. A backup count of `0` discards the current content and removes existing
numeric generations. Purge failures are logged, while browser error responses
remain generic and do not expose filesystem exception details.
PBGui replaces the current file during managed purge/trim operations so live
viewers detect the reset. An arbitrary external program that truncates and
refills the same inode between viewer polls remains outside this guarantee.

## Troubleshooting

- **No log files listed**: make sure PBGui services have been started at least once
- **Streaming stops**: PBAPIServer WebSocket connection lost — the viewer reconnects automatically
- **Max (50,000) is slow**: use a smaller line count for large or fast-moving logs

---

## Where to find what

All log files are located below PBGui's canonical `data/logs/` directory. The
location is anchored to the PBGui installation directory and does not depend
on the process working directory. Use the sidebar to open any file directly.

PBGui serializes concurrent append, rotation, and purge operations across its
threads and processes. Rotation settings are stored atomically in the PBGui
`pbgui.ini`. A per-log override applies to the physical file, so every helper
grouped into `PBGui.log` uses the same rule.

PBGui-owned transcripts use dedicated subdirectories below the same root:

```text
data/logs/jobs/
data/logs/backtests/
data/logs/optimizes/
data/logs/vps-manager/
data/logs/ohlcv-preloads/
data/logs/monitor-agent/
```

The Managed Logs settings apply even before a family creates its first file.
Rotation for child-process captures is performed only before opening a new
capture, so PBGui never renames a file while the child still owns its file
descriptor.

PB7-native bot logs remain in PB7's own `logs/` directory, and legacy
Passivbot stderr remains with its instance runtime directory. PBGui can display
those files but does not claim their storage or rotation ownership.

### Automatic migration cleanup

At the first API start after an update, process-safe startup migrations remove
only explicitly retired PBGui log names and obsolete `income_other_*.json`
diagnostics. Completion is recorded atomically in
`data/state/startup_migrations.json`, so every Master runs the migration once.
Failed migrations remain pending for the next API start. Symlinks and paths
outside the approved roots are never removed.

### Security and context

Browser access to the Logging page uses the same-origin HttpOnly session
cookie. Session tokens are not rendered into the page or JavaScript.
The API path remains relative to the current browser origin and retains the
configured ASGI mount prefix, so an incoming Host header cannot redirect log
requests.

The central logger redacts common credentials from messages, tags, codes,
URLs, exceptions, tracebacks, and nested metadata. This includes passwords,
API keys and secrets, access/session/refresh tokens, authorization and cookie
headers, sensitive query parameters, and private-key blocks. Redaction is a
last safety layer; callers must still avoid logging known secrets.

Operational events may include structured JSON context at the end of a line:

- `request_id` and `operation` for API requests
- `host` for remote/VPS actions
- `instance` or `user` for bot-specific actions

API responses include `X-Request-ID`, allowing an error response to be matched
to its logs without exposing a session identifier.

### Log ownership

PBGui uses three ownership tiers:

1. Independent daemons write dedicated service logs.
2. Data pipelines and detached jobs use dedicated pipeline logs or documented
   transcripts.
3. API/UI helpers without an independent lifecycle share `PBGui.log`.

Machine-readable worker output, installer/maintenance CLI output, raw child
stderr, and user-visible VPS/job transcripts are intentional exceptions. They
are not application loggers and are protected by repository policy tests.

### PBGui.log

Contains messages from grouped API and GUI helper components:

| Component | What you find there |
|-----------|-------------------|
| VPSManager | VPS connections and task coordination |
| Config | Configuration helper errors |
| ParetoDataLoader | Pareto result loading |
| Status | Status helper events |
| HyperliquidAWS | Hyperliquid AWS integration |
| API/UI helpers | Authentication, live sessions, users, API-key state, logging, balance, coin data, dashboard, services, V7 instances, Market Data and PB7 OHLCV actions |

### Dedicated log files

| File | Service | What you find there |
|------|---------|-------------------|
| `PBCluster.log` | PBCluster | Cluster Sync daemon activity and peer sync diagnostics |
| `PBRun.log` | PBRun | Live bot start/stop, order loop |
| `PBCoinData.log` | PBCoinData | CMC data updates, symbol lists |
| `VPSMonitor.log` | VPS Monitor | SSH connections, host metrics, service auto-heal |
| `PBApiServer.log` | PBAPIServer | FastAPI startup, REST/WebSocket requests |
| `Database.log` | Database | DB queries, connection errors |
| `Exchange.log` | Exchange | Market fetch, symbol info, CCXT errors |
| `PBData.log` | PBData | OHLCV download, market data pipeline |
| `SSH.log` | SSH pool | AsyncSSH connections and host-key diagnostics |
| `tradfi_sync.log` | TradFi Sync | TradFi symbol mapping and synchronization |

Additional exchange downloaders, queues, and detached pipelines may expose
their own dedicated log or job transcript. `OptimizeQueueAPI` intentionally
remains dedicated rather than being grouped into `PBGui.log`.
