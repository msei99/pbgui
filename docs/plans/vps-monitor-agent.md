# VPS Monitor Agent Plan

## Goal

PBGui must keep the current VPS Manager monitoring behavior and data quality while avoiding duplicate expensive measurements when multiple masters monitor the same VPS.

The target architecture is a small local monitor service on every VPS runner. The VPS measures itself once and writes cache files. Every master reads those cache files over the normal monitor SSH connection. Masters must not start expensive monitoring scripts on the VPS after this feature is released.

## Problem

Today each master with VPS Monitor enabled starts its own remote collection work against every monitored VPS.

Current expensive or frequent work includes:

- Live CPU/RAM/Disk/Swap and bot CPU sampling every 1 second.
- V7 instance, bot and log metadata collection every 30 seconds.
- Host metadata collection every 30 seconds.
- Service checks every 60 seconds.
- Package status checks every 3600 seconds.

With multiple masters this multiplies the workload on the VPS. The SSH transfer itself is not the problem; duplicated local measurements and scans are the problem.

## Non-Goals

- Do not replicate monitoring data through the Cluster Sync oplog.
- Do not make PBCluster responsible for monitoring collection.
- Do not let monitoring block Cluster Sync.
- Do not let Cluster Sync block monitoring.
- Do not include secrets in monitor cache files.
- Do not let the monitor agent start/stop bots, write V7 configs, write API keys, update PBGui, or modify cluster state.
- Do not keep legacy direct remote collection as a released fallback.

## Target Service

Install a user systemd service on every VPS:

```text
pbgui-monitor-agent.service
```

The service runs a lightweight Python process from the PBGui checkout, for example:

```bash
python monitor_agent.py
```

The exact Python path and PBGui directory must use the configured VPS install directory. Do not hardcode `~/software` in the service template.

## Runtime Files

The agent writes runtime cache files under the remote PBGui directory:

```text
data/monitor_agent/
  live_metrics.ndjson
  live_metrics.latest.json
  instance_snapshot.json
  host_meta.json
  service_status.json
  package_status.json
  collector_status.json
```

Rules:

- Snapshot JSON files must be written atomically with temp file plus `os.replace`.
- `live_metrics.ndjson` is append-only and must be trimmed or rotated periodically in a way that keeps `tail -F` readers working. Prefer rotate-by-rename plus new file creation over in-place truncation.
- Every JSON payload must include `schema_version`, `generated_at`, and enough source metadata to detect staleness.
- Cache files must not contain API keys, VPS passwords, private keys, or full unrestricted log tails.

## Preserve Current Behavior

The first implementation must mirror the current monitor outputs as closely as possible so the UI behavior stays the same.

| Current mechanism | New mechanism |
| --- | --- |
| `MONITOR_AGENT_SCRIPT` started over SSH by each master | local monitor agent writes `live_metrics.ndjson` and `live_metrics.latest.json` |
| `INSTANCE_COLLECT_SCRIPT` started over SSH by each master | local monitor agent writes `instance_snapshot.json` |
| `HOST_META_SCRIPT` started over SSH by each master | local monitor agent writes `host_meta.json` |
| remote service checks started over SSH by each master | local monitor agent writes `service_status.json` |
| `PACKAGE_STATUS_SCRIPT` started over SSH by each master | local monitor agent writes `package_status.json` |

After release, masters read cache files only. If a cache file is missing or stale, PBGui must show a missing/stale monitor-agent state instead of starting the old expensive script.

## Intervals

Keep behavior equivalent to the current monitor where it matters.

| Data | Agent interval | Stale after | Notes |
| --- | ---: | ---: | --- |
| Live CPU/RAM/Disk/Swap | 1s | 5s | Required for current live UI behavior. |
| Bot CPU/RSS/Swap | 1s | 5s | Read from `/proc`; avoid repeated `ps aux` where practical. |
| Live metrics file trim/rotate | 60s | n/a | Keep about 5 minutes of NDJSON samples. |
| V7/instance snapshot | 30s | 90s | Mirrors `INSTANCE_COLLECT_INTERVAL`. |
| Bot error/traceback summary | 30s | 120s | Initially mirror current behavior; later optimize with cursors. |
| Bot PNL/fill summary | 60s | 180s | Prefer incremental processing. |
| Host metadata | 30s initially | 180s | Mirrors current `HOST_META_INTERVAL`; can later relax selected fields to 60s+. |
| Service status | 60s | 120s | Mirrors `SERVICE_CHECK_EVERY * LOOP_INTERVAL`. |
| Package status | 3600s | 7200s | Apt simulation remains expensive. |
| Collector heartbeat/status | 5s | 15s | Allows UI to detect a dead agent quickly. |

CPU and bot CPU must remain 1-second data.

## Live Metrics Payload

`live_metrics.latest.json` and each `live_metrics.ndjson` line should match the current live metrics shape plus schema metadata:

```json
{
  "schema_version": 1,
  "generated_at": 1234567890.123,
  "ts": 1234567890.123,
  "cpu": 14.2,
  "cpu_60s": 11.8,
  "cpu_60s_window": 60.0,
  "cpu_60s_samples": 61,
  "mem": [17179869184, 8589934592, 50.0, 8589934592],
  "disk": [107374182400, 53687091200, 53687091200, 50.0],
  "swap": [0, 0, 0, 0],
  "mem_60s_peak": 70.1,
  "mem_60s_window": 60.0,
  "disk_60s_peak": 55.3,
  "disk_60s_window": 60.0,
  "swap_60s_peak": 0,
  "swap_60s_window": 0,
  "bots": [
    {
      "name": "bot_name",
      "cpu": 2.1,
      "cpu_60s": 1.7,
      "cpu_60s_window": 60.0,
      "rss_mb": 210.4,
      "swap_mb": 0
    }
  ]
}
```

## Instance Snapshot Payload

`instance_snapshot.json` should preserve the current `INSTANCE_COLLECT_SCRIPT` output shape:

```json
{
  "schema_version": 1,
  "generated_at": 1234567890,
  "monitors": [],
  "v7": [],
  "bot_logs": {},
  "cache": {}
}
```

This lets `VPSMonitor` update `instances`, `v7_instances`, and `bot_logs` with minimal behavior changes.

## Host Meta, Services, and Package Payloads

`host_meta.json` should contain the same fields currently returned by `HOST_META_SCRIPT`.

`service_status.json` should contain service rows compatible with the existing `store.update_services(...)` path for PBCluster, PBRun, PBData, and PBCoinData.

`package_status.json` should contain:

```json
{
  "schema_version": 1,
  "generated_at": 1234567890,
  "upgrades": "0",
  "reboot": false
}
```

## Collector Status Payload

`collector_status.json` is the health contract between masters and the local agent:

```json
{
  "schema_version": 1,
  "hostname": "manibot40",
  "agent_version": "1",
  "generated_at": 1234567890,
  "loops": {
    "live_metrics": {"interval": 1, "last_ok": 1234567890, "last_error": ""},
    "instances": {"interval": 30, "last_ok": 1234567880, "last_error": ""},
    "host_meta": {"interval": 30, "last_ok": 1234567880, "last_error": ""},
    "services": {"interval": 60, "last_ok": 1234567860, "last_error": ""},
    "package_status": {"interval": 3600, "last_ok": 1234560000, "last_error": ""}
  }
}
```

The UI should use this to show whether the monitor agent is OK, stale, or missing.

## Master-Side Behavior

`master/async_monitor.py` becomes cache-only for monitored VPS data.

Live metrics:

1. Start an SSH command that tails `data/monitor_agent/live_metrics.ndjson`.
2. Parse each NDJSON line exactly like the current live metrics output.
3. If the stream ends or becomes stale, mark the metrics stale and reconnect the tail.
4. Do not start the old `MONITOR_AGENT_SCRIPT` after release.

Instance data:

1. Read `data/monitor_agent/instance_snapshot.json` over SSH.
2. Validate schema and staleness.
3. Update the existing stores from `monitors`, `v7`, and `bot_logs`.
4. If missing/stale, mark instance data stale. Do not start `INSTANCE_COLLECT_SCRIPT` after release.

Host metadata:

1. Read `host_meta.json` and `package_status.json` over SSH.
2. Validate staleness.
3. Merge the package payload into host metadata as today.
4. If missing/stale, mark host meta/package stale. Do not start old scripts after release.

Services:

1. Read `service_status.json` over SSH.
2. Validate staleness.
3. Update the existing service store.
4. If missing/stale, mark services unknown/stale. Do not run direct service checks after release.

## No Released Legacy Fallback

During development it is acceptable to temporarily keep a fallback to old remote scripts for local testing and staged migration.

Before release:

- Remove the fallback from active runtime paths, or hard-disable it.
- Verify no master starts the old expensive scripts when agent cache is missing.
- Missing/stale cache must become a visible monitor-agent error, not an implicit legacy collection.

This is required because all VPS and masters will be updated as part of the rollout.

## Deployment Requirements

The normal PBGui update path must install and maintain the agent.

VPS setup must:

1. Deploy `monitor_agent.py` with PBGui.
2. Install or update `pbgui-monitor-agent.service` for the configured user and install directory.
3. Run `systemctl --user daemon-reload`.
4. Run `systemctl --user enable --now pbgui-monitor-agent.service`.
5. Verify the service is active.

VPS PBGui update must:

1. Update `monitor_agent.py`.
2. Update the unit file if the template changed.
3. Run `systemctl --user daemon-reload`.
4. Restart `pbgui-monitor-agent.service`.
5. Verify the service is active.

This must be part of the normal **Update PBGui** and **Update PBGui and PB7** VPS workflows so updating all VPS hosts through the existing VPS Manager actions installs the agent without any separate manual step.

Purge must:

1. Stop and disable `pbgui-monitor-agent.service`.
2. Remove the unit file.
3. Run `systemctl --user daemon-reload`.
4. Optionally remove `data/monitor_agent/` with the rest of the PBGui install data.

## Service Unit Template

The unit must be generated with the real install paths. Example only:

```ini
[Unit]
Description=PBGui Monitor Agent
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/home/USER/software/pbgui
ExecStart=/home/USER/software/venv_pbgui/bin/python /home/USER/software/pbgui/monitor_agent.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=default.target
```

Do not hardcode `/home/USER/software`; use the VPS Manager install-dir and Python path logic used by the other PBGui services.

## UI Requirements

VPS Manager should expose monitor-agent state per VPS:

- `Monitor Agent: OK / stale / missing / error`
- `Source: agent cache`
- `Live age`
- `Instances age`
- `Host meta age`
- `Services age`

If the agent is missing:

- Show guidance to run **Update PBGui** or setup/service migration on that VPS.
- Do not silently start legacy direct collection.

If the agent is stale:

- Show guidance to inspect `systemctl --user status pbgui-monitor-agent.service` and `journalctl --user -u pbgui-monitor-agent.service`.

## Implementation Phases

### Phase 1: Live Metrics

- Add `monitor_agent.py` live metrics loop.
- Write `live_metrics.latest.json` and `live_metrics.ndjson` every second.
- Install service through setup/update.
- Make masters read `live_metrics.ndjson` over SSH.
- Keep temporary development fallback only until release.

### Phase 2: Instance Snapshot

- Move `INSTANCE_COLLECT_SCRIPT` behavior into the agent.
- Write `instance_snapshot.json` every 30 seconds.
- Make masters read `instance_snapshot.json` only.

### Phase 3: Host Meta, Services, Package

- Move host-meta behavior into the agent.
- Move service-status behavior into the agent.
- Move package-status behavior into the agent.
- Make masters read cache files only.

### Phase 4: Remove Active Legacy Collection

- Remove or hard-disable old direct collect runtime paths.
- Verify missing/stale agent produces UI warnings, not remote script execution.
- Update guides and release notes.

## Testing Plan

Unit tests:

- Live CPU delta calculation.
- Bot process metric parsing.
- Atomic JSON writes.
- NDJSON trim/rotation.
- Stale detection for every cache file.
- Collector status updates on success and failure.

Monitor tests:

- Fake SSH returns agent cache files and `VPSMonitor` updates existing stores.
- Fake SSH missing cache causes stale/missing status without old script execution.
- Live metrics tail parsing keeps existing UI data shape.
- Instance snapshot parsing preserves current monitor/v7/bot log behavior.

Deployment tests:

- Setup playbook installs/enables/starts the unit.
- Update playbook updates/restarts the unit.
- Purge playbook disables/removes the unit.

UI tests:

- Agent OK/stale/missing labels render correctly.
- Missing agent guidance points users to Update PBGui/service status.

Regression tests:

- Multiple masters reading one VPS use only `cat`/`tail` cache commands.
- No old expensive script string is sent over SSH after release.

## Release Checklist

- `pbgui-monitor-agent.service` is installed by VPS setup.
- `pbgui-monitor-agent.service` is installed/restarted by VPS Update PBGui.
- Purge removes the service.
- Masters read agent cache only.
- Missing/stale agent state is visible in VPS Manager.
- No active legacy direct collect fallback remains.
- Docs/help and German docs are updated.
- `api/serial.txt` is bumped if API/startup/runtime code changed.
- Full relevant test suite passes.
