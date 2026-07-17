# Cluster Sync

Cluster Sync keeps multiple PBGui masters and VPS runners on the same desired V7 and API-key state without using an external storage service.

Use it when you run more than one master, run bots across multiple VPS, or want VPS nodes to keep enough local state to reboot safely when no master is online.

If you are upgrading an existing PBRemote/API Sync/V7 SSH Sync setup, read [Cluster Mode Migration](40_cluster_migration.md) before joining production VPS nodes.

---

## What a cluster is

A cluster is a group of PBGui installations that share one replicated cluster state.

| Term | Meaning |
|---|---|
| **Cluster** | The whole PBGui sync group. It has one stable `cluster_id`. |
| **Node** | One PBGui installation. A node can be a master or a VPS runner. |
| **Master** | PBGui server used to manage configs, API keys, VPS nodes and sync state. |
| **VPS runner** | Server that can run PB7 bots and store a local copy of cluster state. |
| **Desired state** | The cluster decision about which bot should exist, where it should run, and whether it should run. |
| **Operation log** | Append-only history of cluster changes. PBGui rebuilds desired state from it. |

Each node has a stable `node_id`. This ID does not change when the hostname, IP address, SSH port, VPS Manager name or `pbname` changes.

---

## What Cluster Sync covers

Cluster Sync covers:

- V7 bot configs, including coin override JSON files.
- V7 desired state: start, stop, move, delete and tombstone.
- Explicit V7 forced-mode config changes such as Panic, Graceful Stop and Take Profit Only.
- API-key distribution for `api-keys.json`.
- CMC pool credentials for all active state replicas and TradFi vault profiles for masters.
- Local state replicas on masters and VPS nodes.
- Restricted cluster-sync SSH keys for node-to-node replication.

Cluster Sync does not cover:

- DB Tools row sync or database file copy.
- Dashboard and template sync.
- Automatic panic decisions, automatic forced selling or duplicate-bot liquidation behavior.
- Automatic failover moves by itself.

---

## How changes move through the cluster

PBGui does not treat missing files as a delete. Every important change is written as an explicit operation.

Examples:

- Saving or activating a V7 config writes an upsert operation.
- Setting Panic, Graceful Stop or Take Profit Only through Dashboard or Run Config writes and syncs a config operation.
- Moving a bot writes a move operation.
- Stopping a bot writes a stop operation.
- Deleting a bot writes a delete or tombstone operation.
- Updating `api-keys.json` writes an API-key operation.
- Adding, rotating, disabling, or deleting a CMC/TradFi vault entry writes signed credential protocol v2 operations and sealed blobs.

Each node compares its known operation counters with another node. Missing operations and required blobs are transferred, then the receiving node rebuilds its local desired state.

This makes sync repeatable and safe to retry.

Credential protocol upgrades are zero-order. Every updated process first keeps
its own legacy CMC/TradFi credentials available through a local owner-only
shadow record, without changing the legacy source or publishing it. Mixed v1/v2
clusters stay in the passive **waiting for upgrade** state. No freeze, inventory,
or deletion starts until every active state replica advertises v2; disabled and
removed nodes are ignored. The final v2 sync automatically starts cutover, with
no designated last node or service restart.

---

## V7 bots and desired state

For each V7 instance, desired state stores:

- the current config version
- whether the bot should be running or stopped
- the assigned node that is allowed to run it
- a manifest hash of all syncable config JSON files
- conflict status
- tombstone status for deleted bots

PBRun checks desired state before starting a bot.

PBRun starts a bot only when:

- the bot exists in desired state
- it is not tombstoned
- it is not conflicted
- desired state says `running`
- the assigned node matches the local node
- the local config version and manifest hash match desired state

If a check fails, PBRun does not start the bot and PBGui shows the blocked reason.

Panic, Graceful Stop and Take Profit Only are explicit PB7 config changes. Cluster Sync distributes them like any other V7 config update. They are not direct exchange orders and they are not automatic panic decisions made by Cluster Sync.

---

## Bot moves and deletes

Moves are explicit. If a bot is moved from one VPS to another, the old VPS must not start it after learning the move operation.

Deletes are explicit. PBGui never deletes a local V7 instance just because it is missing from a remote file or remote status list.

Tombstones prevent old configs from being brought back by stale nodes.

---

## Offline nodes and reboots

A VPS can reboot even when no master is online.

Boot behavior:

1. The VPS starts PBGui/PBRun.
2. Cluster Sync tries to contact known peer nodes for a short time.
3. If peers are reachable, the VPS pulls missing operations and rebuilds desired state.
4. If no peers are reachable, the VPS uses its local desired state.
5. PBRun starts only bots assigned to this VPS.

Stale local state is warning-only. PBRun does not block startup only because the local cluster state is old.

This is intentional: a host may be offline for a few hours at night, and without automatic failover PBGui should not stop normal reboot recovery just because state is stale.

### Checkpoints and bounded history

PBCluster can replace old operation history with a verified checkpoint plus a
recent operation tail. Cleanup is disabled by default. Open **Cluster Sync ->
Retention** to view the cluster-wide policy and run a read-only report.

The values in **Mode** and **History Days** are drafts until you click **Save
Signed Policy**. **Run Report** always uses the effective signed policy shown in
the blue summary, not unsaved field values. To compare another history window
without permitting deletion, keep **Report only**, enter the new number of days,
save the signed policy, wait until the blue summary shows the new value, and
then run the report again. The allowed history window is 1 to 3650 days; the
default is seven days.

The available modes are:

- **Report only**: default; builds and verifies checkpoints but never deletes history.
- **Prune oplog history**: retains the configured number of days and the current/previous checkpoints.
- **Prune oplog and unreachable blobs**: additionally removes blobs that remain unreachable in two identical reports for at least 24 hours.

The report columns mean:

- **Status**: `dry_run` means the node only calculated candidates and did not delete anything.
- **Checkpoint**: deterministic shadow-checkpoint ID calculated from that node's current validated state and effective policy.
- **Eligible Ops**: operation files old enough for the effective history window and at or below the checkpoint baseline.
- **Eligible Size**: combined on-disk size of those eligible operation files; it does not include configs, credentials, checkpoints, or blobs.
- **Retained Ops**: operation files that remain in the recent tail.
- **Blob Candidates** and **Blob Size**: projected unreachable blobs and their combined size before cleanup, or the values from the latest matching automatic GC evaluation after cleanup starts.
- **Blob GC Status**: `projected` for the read-only pre-cleanup simulation, or whether automatic blob GC is blocked by a safety gate, ready, or complete. The status also lists blockers such as a missing committed checkpoint or the 24-hour stability window and reports already deleted blob counts and bytes after completion.
- **Migration Seal / Error**: local seal result or a node error. `not reported` means the remote preview does not expose its seal result; the commit protocol still verifies every replica's seal independently.

Values are per node. Equal rows normally describe the same replicated operation
set on each node and must not be added together as different cluster
operations. Eligibility requires both the signed operation timestamp and the
local durable file age to be older than the cutoff.
Blob candidates describe each node's local content-addressed stores and may
legitimately differ when one node has additional orphaned copies. Checkpoint ID,
Eligible Ops, and Retained Ops must still converge across replicas.

Before cleanup is enabled, **Run Report** projects blob candidates from the
shadow checkpoint, the current/previous checkpoint protection, a simulated
operation prune using the effective history window, the retained operation
tail, and live mailbox references. `projected` values are therefore a preview;
blockers such as `checkpoint_missing` still prevent deletion. Once a matching
automatic GC evaluation exists, the table shows its actual candidates and
stability status instead. **Run Report** never persists candidates, advances
the 24-hour stability window, or deletes data.

After saving a policy, mixed rows are normal for a short time while PBCluster
replicates the new signed operation. Different checkpoint IDs or different
eligible/retained counts mean the replicas are not yet converged. Do not enable
cleanup in that state. Wait for a completed PBCluster sync cycle and rerun the
report. Before enabling cleanup, every active replica must be available, show
the same checkpoint ID and operation candidate counts, and report no error; the
local migration seal must show `sealed` and the Cluster page must show no
conflicts. Blob candidate counts may differ for the local-store reason described
above.

A safe first cleanup rollout is:

1. Keep **Report only**, save the desired history window, and run reports until all active replicas match.
2. Select **Prune oplog history** when only operation history should be cleaned, or select **Prune oplog and unreachable blobs** directly when both should be cleaned. Save the signed policy.
3. Wait for PBCluster to propose the checkpoint and collect matching signed acknowledgements from every active state replica.
4. Verify that the blue summary shows an active committed checkpoint. `no committed checkpoint yet` means deletion is still blocked.
5. Wait at least 24 hours after the destructive policy became active. Cleanup is evaluated on policy changes, at the 5,000-operation or 10-MiB soft trigger, and at least daily so age-only transitions are eventually processed.
6. Rerun the report and verify cluster state, V7 assignments, credentials, CMC, and TradFi after the oplog cleanup.
7. In the combined mode, inspect **Blob Candidates**, **Blob Size**, and **Blob GC Status** after the first automatic GC evaluation. Blob deletion remains independently blocked until two identical candidate reports are at least 24 hours apart.

Changing the policy writes a signed cluster operation. A destructive mode does
not bypass safety checks: every active state replica must independently confirm
the same checkpoint, credential protocol v2 migration must be sealed, the
checkpoint reducer must match full replay, and the destructive policy must be
active for 24 hours. A conflict automatically falls back to report-only.

Operations after a checkpoint are signed and bound to its checkpoint ID. A
node behind deleted history installs the proven checkpoint and required blobs
before syncing the tail. PBGui rejects a divergent stale tail instead of
merging it. Checkpoint-aware Join and Join Existing Cluster do not require old
genesis operation files.

---

## Conflicts

A conflict can happen when two masters change the same bot from the same parent version before they sync with each other.

When PBGui detects a conflict:

- the instance is marked conflicted
- PBRun must not auto-start it
- the Cluster page shows the competing operations
- the user must choose or create the winning version
- the resolution writes a new operation

PBGui does not use blind last-write-wins for V7 instance conflicts.

---

## Credentials

Cluster Sync tracks both exchange `api-keys.json` updates and credential-vault generations.

The desired state stores only metadata such as serial and payload hash. The API-key content is stored as restricted secret data and must not appear in logs or normal desired-state JSON.

Installing API keys on a node uses the Cluster Sync materialization safety steps:

- create a backup on master nodes when an existing file differs
- skip local backups on VPS runner nodes
- write the new file
- verify the payload
- do not restart bots or deploy other files

CMC and TradFi vault entries do not use the exchange API-key blob. Credential protocol v2 signs each operation and seals each secret generation to its eligible recipients. CMC uses the `cluster` audience (active masters and VPS replicas); TradFi uses the `masters` audience. A VPS can validate, store, and forward an opaque TradFi envelope but is not a recipient and cannot decrypt it.

Protocol-v1 peers never receive v2 credential operations. Credential migration and new credential publication wait until every active state replica reports protocol v2 crypto capability.

CMC leases are best-effort coordination metadata, not a dependency for using a materialized key. If an authority or relay is unavailable, local soft-budget selection continues. Provider `429` responses cool down the affected key and fail over to another eligible pool key when possible. Imported, externally used, and shared-quota keys are valid pool members; provider rotation is optional.

When active membership changes, PBGui rewraps existing secret generations for the new recipient set. A new node must not report credential capability as active until rewrap, exact-generation materialization, and acknowledgement complete. TradFi remains master-only during this process.

---

## Cluster-sync SSH keys

Cluster Sync uses dedicated restricted SSH keys instead of normal admin SSH keys for regular replication.

Admin SSH credentials are used only for bootstrap, key installation and recovery.

Cluster-sync keys are restricted so they cannot open a normal shell or run arbitrary commands. They are installed with an OpenSSH forced command that only allows cluster-sync actions such as reading the state vector, sending operations, sending blobs and rebuilding desired state.

This limits the damage if a cluster-sync key is leaked: the key should not allow interactive login, port forwarding, agent forwarding or unrestricted SFTP.

---

## VPS-to-VPS firewall rules

Cluster Sync manages VPS-to-VPS SSH firewall rules automatically for peer sync.

PBGui adds allow rules for enabled peer VPS nodes that need to exchange cluster state. Old PBGui-managed peer rules are removed only after replacement connectivity has been confirmed.

PBGui must not remove SSH firewall rules that were not created for Cluster Sync.

---

## Cluster page

The dedicated **Cluster Sync** page is the main place to monitor Cluster Sync.

The page is split into Overview, Setup, Nodes, Credentials, V7 State, Tombstones, Retention and Oplog sections. It refreshes local status, nodes, desired state and recent oplog entries in the background, and updates changed cards and node-table fields in place instead of reloading the whole screen.

The page shows:

- cluster identity and local node identity
- all materialized nodes and their roles
- V7 desired state
- conflict and tombstone status
- API-key metadata when present
- recent local operation-log entries
- an explicit Join Existing Cluster action for a second master that can reach an existing master outbound
- a bootstrap preview/apply action for known VPS nodes and existing local V7 configs
- read-only remote hello probe status for known cluster nodes
- an explicit Join & Sync action for reachable nodes without cluster identity
- a read-only Preview action for joined nodes that compares remote state for diagnostics or retry
- editable node sync mode, SSH endpoint, Remote PBGui Dir and outbound peer allowlist
- disabled-node removal for stale nodes that no longer own V7 configs
- signed history-retention policy and bounded read-only per-node cleanup reports

Bootstrap writes explicit local `ADD_NODE` operations for known VPS Manager hosts and `UPSERT_CONFIG` operations for local configs. When VPS Monitor metadata is available, Bootstrap preserves whether a known host is a master or VPS runner. It never infers deletes from missing files or missing VPS entries and it does not clear tombstones. The probe column runs a read-only restricted `hello` command when available; it does not install keys, write remote files, start bots, stop bots, or deploy anything.

Node sync mode controls which nodes PBCluster may contact:

- **Disabled** keeps the node in cluster history but excludes it from sync.
- **Outbound Only** means the node does not need inbound SSH; it can still initiate sync to allowed peers.
- **Reachable via SSH** lets allowed peers contact the node through its SSH host, SSH user, SSH port and Remote PBGui Dir.

The local master detects its own Remote PBGui Dir from the running checkout and stores it as a home-relative path when possible. It also fills missing local SSH host/user metadata from the local network and login user. Review remote node metadata after join or import, especially when a private VPN address is preferred over a public address.

After bootstrap or import, VPS nodes often start as **Disabled**. First use **Edit**, switch the node to **Reachable via SSH**, verify SSH host/user/port and Remote PBGui Dir, save, then run **Repair SSH**. After that, click **Probe Active Nodes**. The **Join & Sync** button is shown only after PBGui has a fresh probe status and the reachable node reports **No Identity**.

When a node shows **No Identity**, **Join & Sync** writes the remote Cluster identity and refuses to overwrite a different existing identity. It then pushes missing local operations, rebuilds remote Cluster state, materializes assigned V7 configs and API keys, and starts PBRun again when the remote is current. On VPS runners, Join stops PBRun first so running bots are not evaluated during the transition; passivbot processes are left alone. On master nodes, PBRun is not stopped or started.

After protocol-v2 operations arrive, PBCluster also materializes eligible sealed credentials into the owner-only vault. Exchange `api-keys.json` materialization and sealed credential materialization are separate steps.

To add a newly set up VPS runner to an existing cluster, use this order:

1. Add and set up the VPS in **System -> VPS Manager**. After a successful setup PBGui records the VPS locally as a Cluster node candidate automatically. If the host was set up before this automation existed, open the VPS in VPS Manager and click **Add to Cluster**.
2. Open **System -> Cluster Sync -> Nodes** and find the new VPS row.
3. Use **Edit** on the new VPS row, set **Sync Mode** to **Reachable via SSH**, verify SSH host/user/port and Remote PBGui Dir, then save.
4. Click **Probe Active Nodes**. The node should become reachable and show **No Identity** before join.
5. Click **Join** / **Join & Sync** on the VPS row. This writes the remote Cluster identity and syncs/materializes state on the VPS.
6. Edit the local master node that should actively synchronize with this VPS and add the new VPS to that master's allowed sync peers. Without this step, the new VPS can be joined but the master's **Login Key** column may show **Skipped** because PBCluster is not trying to contact that node from this master.
7. Run **Install Key** on the new VPS row, or **Repair All SSH** after adding several nodes or changing peer lists.
8. Click **Probe Active Nodes** again and wait for PBCluster to run one sync pass. **Login Key** should change from **Skipped** or **Checking** to **Installed** once the local master has successfully logged in with the dedicated Cluster Sync key.

The **Login Key** column describes the regular PBCluster sync login, not the join result. **Skipped** means the node is not currently in the local outbound sync topology, for example because the local master's sync peer list does not include that VPS yet. It does not mean that Join failed.

Use **Join Existing Cluster** on a second master that cannot be reached inbound by the primary master but can SSH out to it. Do this before Bootstrap when the master should join an existing cluster. The action uses the VPS Monitor SSH pool when key login already works, or prompts for a one-shot SSH password before keys are installed. It searches for the upstream PBGui directory in the same order as VPS Manager (`remote_pbgui_dir`, `~/software/pbgui`, `~/pbgui`), reads the upstream master, automatically adopts the upstream `cluster_id` when the local oplog is empty, pulls upstream operations and blobs, registers the local master as `outbound_only` with detected local path/IP/user metadata, installs the local Cluster SSH key on the upstream master and pushes the registration operations back. The SSH password is used only for that request and is not saved. If this local install already has cluster operations for a different cluster, self-join refuses to overwrite them unless the recovery option is enabled. Recovery archives the previous local cluster state under `data/cluster/archives/` before replacing it with the upstream cluster state. After join, switch that master to **Reachable via SSH** only when other allowed peers should initiate SSH back to it.

PBCluster SSH access is technical setup state. During normal PBGui setup/update on a VPS, PBGui now creates a dedicated local PBCluster SSH key and installs the master's public key on the VPS with a forced command that can only run `cluster_sync_command.py`. PBCluster uses this dedicated key with `IdentitiesOnly=yes`; users do not need to create or copy SSH keys manually.

VPS nodes do not initiate SSH fanout to other peers by default. A runner VPS only contacts explicit `sync_peers`; this avoids accidental VPS-to-VPS meshes. Masters can still push to reachable VPS nodes unless their outbound peer list is explicitly restricted.

Use **Edit** on a node to configure its sync mode, SSH host/user/port, Remote PBGui Dir and allowed outbound peers. Use **Repair SSH** for one node after changing its peer allowlist, SSH metadata or after updating that node: it reads the remote PBCluster public key, stores its fingerprint in cluster metadata, and installs the required restricted keys for the master and any configured peer sources. Use **Repair All SSH** after a larger update or topology change when several active reachable nodes may need key refresh. It runs the same repair flow for every active node, reports failed nodes, outbound install errors and missing source keys, and leaves disabled/outbound-only inbound targets untouched. If normal SSH key login is not available yet, PBGui prompts for the affected node's SSH password, retries with that password for this request only, and does not save it. Use **Remove** only for disabled non-local nodes that no longer own any V7 configs; it writes a `REMOVE_NODE` operation and removes the node from materialized membership while keeping oplog history intact.

When a joined node shows **OK**, the **Preview** action reads the remote state vector and desired state. It compares actor sequence numbers, V7 instance metadata, tombstones and API-key metadata against local state. It also calculates which local operations the remote is missing, which remote operation ranges are missing locally, and which hash references a later write phase would need. Preview is read-only; it does not copy operations, blobs or configs.

From the Preview window, **Push Missing Ops + Rebuild** is an explicit retry/diagnostic remote write action. It is available only when the remote has no operations missing locally. It starts one backend push job that sends current V7 config blobs, API-key payload blobs, API-key secret blobs, bulk-sends the local oplog entries the remote state-vector lacks, reports local progress while the job runs, and then runs remote `rebuild`. Progress reporting does not split or slow the remote sync. If the remote wrapper is older and cannot accept the bulk commands yet, PBGui falls back to slower per-item uploads where available.

After operations and config blobs are synchronized, the Preview window also shows **V7 Config Materialization Preview**. **Materialize V7 Configs** is the manual retry action for writing assigned, non-conflicted V7 JSON configs from verified config blobs into the remote `data/run_v7` directory. It refuses to run when the remote state-vector or desired state differs from local state, or when required blobs are missing or invalid. It skips configs assigned to other nodes, conflicted configs and tombstoned instances.

The Preview window also shows **API-key Materialization Preview**. **Materialize API Keys** is the manual retry action for installing `api-keys.json` from the replicated secret blob. Master nodes create a normal `data/api-keys/` backup first when an existing file differs; VPS runner nodes skip local backups. The write is atomic and verifies the final hash.

CMC/TradFi vault migration is resumable and automatic. It inventories legacy CMC/TradFi fields, establishes a signed writer freeze across active v2 replicas, imports and seals immutable generations, waits for exact materialization acknowledgements, then backs up and removes unchanged legacy fields. Do not re-add CMC secrets to `pbgui.ini`, per-VPS inventory, or automation, and do not manually edit PB7 TradFi entries. Rotation is not required to finish cleanup.

---

## What to do when something is wrong

If a node is offline:

- Check SSH reachability from the Cluster or VPS Manager page.
- Check whether the node is enabled for sync.
- Check host, port, user and host key metadata.

If a bot does not start:

- Open the Cluster page and check for blocked-start details.
- Verify that the bot is assigned to this node.
- Check for conflict or tombstone state.
- Verify that the local config version matches desired state.

If a conflict appears:

- Do not manually copy files between nodes.
- Review the competing operations on the Cluster page.
- Choose or create the correct winning config.
- Let PBGui write the resolution operation.

If a foreign cluster warning appears:

- Do not force sync.
- Verify that the node belongs to this PBGui cluster.
- Join or reset the remote cluster identity only when you are sure it is the correct node.

If **Repair All SSH** reports outbound errors:

- For `SSH authentication failed`, enter the prompted SSH password for the named node and retry from the modal. The password is temporary and is not saved.
- For `Remote host is unreachable`, verify the node's **Reachable via SSH** metadata and network/firewall access, then run **Probe Active Nodes**.
- For missing source keys, run **Repair SSH** on the source node first or rerun **Repair All SSH** after the source node has a stored Cluster SSH public key.
- After repair, run **Probe Active Nodes** again before using **Join & Sync** or remote Preview actions.

---

## Safety rules

- Do not delete local bot directories to signal a delete. Use PBGui so it writes a delete operation.
- Do not reuse copied `data/cluster/node_id` files on another installation.
- Do not edit `desired_state.json` manually; it is generated from the operation log.
- Keep admin SSH access available for recovery even though cluster replication uses restricted keys.
