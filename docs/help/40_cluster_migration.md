# Cluster Mode Migration

This guide lists the required steps to move an existing PBGui setup from PBRemote/API Sync/V7 SSH Sync to Cluster Sync.

PBRemote is no longer needed and is removed during the upgrade. Cluster Sync takes over V7 config and API-key sync.

Cluster Sync replaces the old sync paths. PBRun is only needed on hosts that run bots. A master-only node needs PBApiServer and PBCluster, but PBRun can stay stopped.

---

## Steps

### 1. Update the primary master

1. Update PBGui on the master you normally use for the UI.
2. Restart `pbgui-api.service` when PBGui shows the restart warning.
3. If this master does not run bots, PBRun can stay stopped.

### 2. Bootstrap Cluster Sync

1. Open **System -> Cluster Sync**.
2. Run **Bootstrap Preview**.
3. If the preview shows the expected local V7 configs and VPS hosts, run **Bootstrap Apply**.

### 3. Join additional masters

1. On each additional master, add the primary master to VPS Manager if it is not already known there.
2. Open **System -> Cluster Sync** on the additional master.
3. Use **Join Existing Cluster** with the primary master's VPS Monitor hostname and SSH details.
4. Confirm adoption only when this additional master has no local Cluster oplog entries yet.

### 4. Update VPS runners

1. Update each VPS runner with **VPS Manager -> Update PBGui**.
2. If VPS Manager shows that systemd migration is needed, run **Systemd Migration Preview** and then **Apply**.
3. Run **Cleanup VPS** afterward to remove old PBRemote/rclone leftovers.
4. Pure VPS runners do not need `pbgui-api.service`.

### 5. Join VPS nodes

1. Open **System -> Cluster Sync**.
2. If a VPS node shows **No Identity**, use **Join**.
3. **Join** writes the Cluster identity, syncs Cluster data, materializes V7 configs/API keys and starts PBRun again when everything is current.
4. For VPS runners, **Join** stops PBRun automatically during this step. The running passivbot processes are left alone.

### 6. Check the result

1. Open **PBv7 -> Run** and **VPS Manager**.
2. If bots are shown as blocked, fix the assignment or config in PBGui.
3. If Join reports that automatic sync/materialization needs attention, open **Preview** for the node and run the suggested action there.

---

## Done

- PBRemote is no longer used.
- API keys and V7 configs are materialized through Cluster Sync.
- `data/cmd/status_v7.json` is no longer created, read, or honored.
