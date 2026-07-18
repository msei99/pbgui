# VPS Monitor

VPS Monitor is the real-time operations dashboard for every configured VPS host. PBAPIServer sends state through a cookie-authenticated WebSocket, so the page updates without polling or exposing the browser session in HTML, JavaScript, URLs, or WebSocket parameters.

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
- **Instances**: running and deployed bot instances with status details
- **Services**: PBCluster, PBRun, PBData, PBCoinData, and PBMonitorAgent status and restart actions
- **Live Logs**: real-time service and bot log streams

## Live log features

- Real file line numbers
- Group collapse and expansion for log blocks
- Full-text search with highlighting
- Auto-scroll and compact mode
- Host and service selectors with stream control

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
- **Logs do not stream**: check `PBApiServer.log` for connection errors.
