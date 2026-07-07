# Cluster Mode Migration

This guide lists the required steps to move an existing PBGui setup from PBRemote/API Sync/V7 SSH Sync to Cluster Sync.

PBRemote is no longer needed and is removed during the upgrade. Cluster Sync takes over V7 config and API-key sync.

Cluster Sync replaces the old sync paths. PBRun is only needed on hosts that run bots. A master-only node needs PBApiServer and PBCluster, but PBRun can stay stopped. Pure VPS runners do not need `pbgui-api.service` or `PBApiServer.py`; they need PBCluster for sync and PBRun only when they run bots.

---

## Steps

### 1. Update the primary master

1. Update PBGui on the master you normally use for the UI.
2. Restart `pbgui-api.service` when PBGui shows the restart warning.
3. PBGui's normal update and branch-switch workflow syncs the PBCluster systemd unit and restarts PBCluster. If you updated with a manual `git pull`, restart it yourself with `systemctl --user restart pbgui-pbcluster.service`.
4. If this master does not run bots, PBRun can stay stopped.

### 2. Bootstrap Cluster Sync

1. Open **System -> Cluster Sync**.
2. Run **Bootstrap Preview**.
3. If the preview shows the expected local V7 configs and VPS hosts, run **Bootstrap Apply**.

### 3. Join additional masters

1. On each additional master, add the primary master to VPS Manager if it is not already known there, or enter its SSH details directly in the Join form.
2. Open **System -> Cluster Sync** on the additional master.
3. Use **Join Existing Cluster** with the primary master's VPS Monitor hostname and SSH details. If Cluster SSH keys are not installed yet, PBGui first tries existing key/pool login and prompts for the SSH password only when needed. The password is used only for that request without saving it.
4. PBGui automatically adopts the primary master's `cluster_id` when this additional master has no local Cluster oplog entries yet.
5. The new master registers as **Outbound Only** by default. Switch it to **Reachable via SSH** only when other allowed peers should initiate SSH back to it.
6. If the master was accidentally bootstrapped first, enable the recovery option. PBGui archives the previous local Cluster state under `data/cluster/archives/` and then joins the primary master's cluster.

### 4. Update VPS runners

1. Update each VPS runner with **VPS Manager -> Update PBGui**. This syncs PBCluster service files and restarts PBCluster, PBRun and PBCoinData where those services are configured.
2. If VPS Manager shows that systemd migration is needed, run **Systemd Migration Preview** and then **Apply**.
3. Run **Cleanup VPS** afterward to remove old PBRemote/rclone leftovers.
4. Pure VPS runners do not need `pbgui-api.service` and should not run `PBApiServer.py`.
5. If you update a runner manually with `git pull`, restart PBCluster afterward with `systemctl --user restart pbgui-pbcluster.service`.

### 5. Join VPS nodes

1. Open the VPS in **System -> VPS Manager**. If it was not registered automatically after setup, click **Add to Cluster**. This writes local Cluster metadata only; it does not SSH to the VPS or join it.
2. Open **System -> Cluster Sync -> Nodes**.
3. Open **Edit** for the VPS node, set **Sync Mode** to **Reachable via SSH**, verify SSH host/user/port and **Remote PBGui Dir**, then save.
4. Click **Probe Active Nodes** and wait until the node is reachable and reports **No Identity**.
5. Use **Join**. Join writes the Cluster identity, syncs Cluster data, materializes V7 configs/API keys and starts PBRun again when everything is current. For VPS runners, Join stops PBRun automatically during this step; running passivbot processes are left alone.
6. Edit the local master node that should sync with this VPS and add the VPS to that master's sync peers.
7. Use **Install Key** on the VPS node, or **Repair All SSH** after updating several nodes or changing peer allowlists across the cluster.
8. If PBGui prompts for an SSH password during key installation or repair, enter the password for the named node. It is used only for that request and is not saved.
9. Click **Probe Active Nodes** again. **Login Key** should become **Installed** after PBCluster has synced once. **Skipped** means the node is not in the local master's outbound sync peer list yet; it does not mean Join failed.

### 6. Check the result

1. Open **PBv7 -> Run** and **VPS Manager**.
2. If bots are shown as blocked, fix the assignment or config in PBGui.
3. If Join reports that automatic sync/materialization needs attention, open **Preview** for the node and run the suggested action there.

---

## Done

- PBRemote is no longer used.
- API keys and V7 configs are materialized through Cluster Sync.
- `data/cmd/status_v7.json` is no longer created, read, or honored.
- PBCluster is running on sync nodes; `pbgui-api.service` runs only on masters that serve the PBGui UI/API.
