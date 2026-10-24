# VPS Monitor


The global navigation alert badge updates automatically while its browser tab is visible. Alert polling pauses in background tabs and resumes immediately when you return, without overlapping requests.

Pending restart and kill buttons remain disabled across monitor refreshes. Controls recover when the response arrives, the request times out, or the connection is lost.

VPS Monitor is the real-time operations dashboard for every configured VPS host. PBAPIServer sends state through a cookie-authenticated WebSocket, so the page updates without polling or exposing the browser session in HTML, JavaScript, URLs, or WebSocket parameters.

## Actionable threshold alerts

Open **VPSMonitor alerts** in the top navigation. Instance alerts name every exceeded metric and show the recorded reading, strict `>` comparison and configured error limit: memory/swap in MB, confirmed 60-second average CPU in %, and today's error/traceback counts. Triggered comparisons retain numeric precision, including readings just above a limit and decimal limits. The current-reading overview remains available. CPU requires fresh metrics and a complete averaging window.

**Adjust Memory threshold**, **Adjust CPU threshold** and the corresponding metric links open **System → PBGUI Services → PBAPIServer → Settings → VPS Monitoring** at the exact error field in **V7 Monitor** (instances) or **Server Monitor** (systems). Links also work from the Services page itself. Loading finishes before the field is focused; browser reload restores the field destination. Unsaved input remains intact during status updates, navigation between threshold fields, and retries after failed settings loads. Edits made while an error is displayed also survive Retry. Returning to a page retained in the browser page cache preserves its draft, focus and threshold destination; interrupted loading resumes only for the active Settings tab. **Save** applies deliberate settings changes; opening a link changes no configuration.

**Ack** and **Ack all** retain their existing behavior. Active alerts update their readings and limits without clearing acknowledgement. History retains the readings and limits captured with the alert; legacy entries without trigger data offer **Threshold settings** without inventing historical limits. An unavailable field stays in Settings with a short message. A failed or timed-out settings load blocks saving until a successful retry.

## What you can monitor

- SSH connection state for each host
- Live CPU, RAM, disk, swap, and bot process metrics
- Service state for PBCluster, PBRun, PBData, PBCoinData, and PBMonitorAgent
- Bot instances and synchronization details across hosts
- Live service and bot logs
- Monitor-agent heartbeat, cache-file health, and collector errors

## Monitor-agent data source

Each VPS runs `pbgui-monitor-agent.service`. It measures the local host once and writes cache files for all connected PBGui masters. VPS Monitor consumes only this monitor-agent cache; there is no direct collector fallback when a cache is missing, stale, or invalid.

The canonical live stream is:

```text
data/monitor_agent/live_metrics.ndjson
```

The complete cache set is:

```text
data/monitor_agent/live_metrics.ndjson
data/monitor_agent/live_metrics.latest.json
data/monitor_agent/instance_snapshot.json
data/monitor_agent/host_meta.json
data/monitor_agent/service_status.json
data/monitor_agent/package_status.json
data/monitor_agent/collector_status.json
```

Snapshot JSON files are replaced atomically. The NDJSON stream uses PBGui-managed, byte-based retention and rotation so its disk use remains bounded while readers continue following the stream.

While live bot telemetry is fresh, an instance snapshot retains the current CPU, RAM, and swap values for the same bot run. This keeps threshold alerts stable between live samples. Without custom `[monitor]` settings, PB7 bot CPU warning and error thresholds are 15% and 20%; the RAM error threshold is 500 MB.

## Agent health

Every host card shows **Monitor Agent: OK, Stale, Missing, Error, or Unknown** and always identifies the source as `monitor-agent cache`.

- Live telemetry is healthy for up to **15 seconds** after its last sample.
- The collector heartbeat is healthy for up to **30 seconds**.
- **Stale** means an effective age exceeded one of those limits.
- **Missing** means a required cache file is reported absent.
- **Error** means the agent, a required file, or a collector loop reported an error.
- **Unknown** means no usable monitor-agent diagnosis has arrived yet.

The detail panel shows heartbeat and effective ages, every required file state, and bounded collector errors. SSH connectivity is independent: a host may remain **connected** over SSH while its monitor-agent telemetry is stale.

## Tabs and workflow

- **Dashboard**: host, SSH, telemetry, and monitor-agent health
- **Instances**: running and deployed PB7/PB8 bot instances with explicit runtime identity, config/running version drift, and stopped or disabled state. Equal PB7/PB8 names remain separate rows, and log and kill actions keep that identity so they cannot target the wrong runtime. A stopped PB8 row is shown only from a fresh explicit monitor-agent observation; stale or unstamped inventory never invents a stopped bot.
- **Services**: PBCluster, PBRun, PBData, PBCoinData, and PBMonitorAgent status and restart actions
- **Live Logs**: real-time service and bot log streams

On explicitly configured slave nodes with automatic service healing enabled, **Services → PBCoinData** shows `idle` after an owned process is confirmed stopped and no running PB7 Dynamic Ignore consumer needs it. A running consumer and ready CMC pool allow automatic recovery. The local agent controls systemd units in its regular 60-second cycle; the unit stays installed. For legacy operation, an initialized current PBRun holds an exclusive controller lease and reconciles in its existing five-second maintenance cycle. The agent can acquire a free lease for one fallback cycle. Master nodes retain credential-based behavior; PB8 live bots do not require this DynamicIgnore daemon.

The displayed process state and demand reason are separate: unknown consumer state triggers neither automatic stop nor start. Recovery metadata identifies the verified local controller separately. Failed or unconfirmed stops retain the actual state and diagnostic. Legacy actions require exact UID, Python script, checkout, working directory, PID and start-time ownership; unknown or foreign processes are never signalled. **Restart** remains an explicit action and is not blocked by PBRun's controller lease. Status updates arrive automatically through the existing monitor stream.

While active, the slave daemon limits automatic mapping work to all known running PB7 Dynamic Ignore consumers' exchanges. Incomplete account or process detection retains all exchanges. Masters and direct Coin Data UI jobs keep full access. Legacy unloading requires verified recovery authority; a PBRun PID alone is insufficient, and a standalone daemon without that authority keeps its waiting idle loop. An installed systemd unit or unknown manager blocks automatic legacy starts. If demand returns during an unfinished systemd transition, the agent observes it instead of issuing a competing restart. See the PBCoinData Service guide for exchange selection, bounded process actions and bootstrap behavior.

A manual **Restart** on an idle slave does not pin the daemon in memory: with automatic healing enabled, its elected local controller unloads it again at the next confirmed IDLE cycle. PBRun's legacy cycle is five seconds and the agent cycle is 60 seconds; service transitions can take longer, and brief intermediate states may pass between UI updates.

If the user bus or systemctl is unavailable, legacy recovery requires a fresh complete offline proof of unit and user-manager absence; bus failure alone is insufficient. Unreadable search paths, live managers or uncertain namespace/process visibility remain unknown, with a reason shown through existing status fields. Actual start failures share a persisted 60–1800-second backoff across PBRun/agent changes. Manual **Restart** may bypass that wait; unknown ownership still blocks actions. See the PBCoinData Service guide for evidence, clock and persistence limits.

Manual Restart uses the shared guarded ownership protocol, without classifying ordinary foreign services. Missing transition evidence alone does not block explicit manual actions; the automatic legacy unloading preflight requires independently confirmed transition. Systemd instead uses fresh concrete unit/MainPID/ControlGroup/checkout/guard evidence without legacy attestation. An ancestor service cgroup alone does not veto manual control; separate legacy origin must also be provable. Without transition, a running legacy idle daemon remains resident; missing transition alone does not block otherwise eligible ACTIVE recovery. Structural blockers and valid cooldowns retain the idle process with a reason; repairable legacy retry content does not veto IDLE. Credentials are checked again at ACTIVE. See the PBCoinData Service guide for the operator-only transition command, copied/unguarded-code boundary and safe pending test sequence. No GUI, real Ubuntu VM, live-systemd or RAM acceptance is claimed.

## Live log features

- Real file line numbers
- Group collapse and expansion for log blocks
- Full-text search with highlighting
- Auto-scroll and compact mode
- Host and service selectors with stream control
- Restart pre-arms the selected log stream so early startup lines remain visible without a manual Fetch

## Requirements

- PBAPIServer must be running.
- Target VPS hosts must be enabled in PBAPIServer settings (`System → Services → PBAPIServer → Settings`).
- `pbgui-monitor-agent.service` must be installed and running on each VPS.
- The PBAPIServer WebSocket endpoint must be reachable from the browser.

## Troubleshooting

Check the monitor agent on the affected VPS:

```bash
systemctl --user status pbgui-monitor-agent.service
journalctl --user -u pbgui-monitor-agent.service
```

Restart it when required:

```bash
systemctl --user restart pbgui-monitor-agent.service
```

- **No data shown**: start PBAPIServer from `System → Services`.
- **One host missing**: confirm the host is enabled in PBAPIServer settings.
- **Agent Missing**: update or migrate the VPS installation so `pbgui-monitor-agent.service` and all cache files are installed.
- **Agent Stale**: inspect the service status and journal. SSH may remain connected while telemetry is stale.
- **Agent Error**: review the bounded collector errors in the host details, then inspect the service journal for the complete local error.
- **Connection lost during an action or history request**: the pending control recovers automatically. Wait for the WebSocket banner to reconnect, then retry the action.
- **Logs do not stream**: check `PBApiServer.log` for connection errors.
