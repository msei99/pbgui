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

Each node compares its known operation counters with another node. Missing operations and required blobs are transferred, then the receiving node rebuilds its local desired state.

This makes sync repeatable and safe to retry.

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

## API keys

Cluster Sync also tracks `api-keys.json` updates.

The desired state stores only metadata such as serial and payload hash. The API-key content is stored as restricted secret data and must not appear in logs or normal desired-state JSON.

Installing API keys on a node uses the Cluster Sync materialization safety steps:

- create a backup on master nodes when an existing file differs
- skip local backups on VPS runner nodes
- write the new file
- verify the payload
- do not restart bots or deploy other files

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

The first local-only version shows:

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

Bootstrap writes explicit local `ADD_NODE` operations for known VPS Manager hosts and `UPSERT_CONFIG` operations for local configs. When VPS Monitor metadata is available, Bootstrap preserves whether a known host is a master or VPS runner. It never infers deletes from missing files or missing VPS entries and it does not clear tombstones. The probe column runs a read-only restricted `hello` command when available; it does not install keys, write remote files, start bots, stop bots, or deploy anything.

When a node shows **No Identity**, **Join & Sync** writes the remote Cluster identity and refuses to overwrite a different existing identity. It then pushes missing local operations, rebuilds remote Cluster state, materializes assigned V7 configs and API keys, and starts PBRun again when the remote is current. On VPS runners, Join stops PBRun first so running bots are not evaluated during the transition; passivbot processes are left alone. On master nodes, PBRun is not stopped or started.

Use **Join Existing Cluster** on a second master that cannot be reached inbound by the primary master but can SSH out to it. Do this before Bootstrap when the master should join an existing cluster. The action uses the VPS Monitor SSH pool when key login already works, or a one-shot SSH password from the form before keys are installed. It searches for the upstream PBGui directory in the same order as VPS Manager (`remote_pbgui_dir`, `~/software/pbgui`, `~/pbgui`), reads the upstream master, automatically adopts the upstream `cluster_id` when the local oplog is empty, pulls upstream operations and blobs, registers the local master as `outbound_only`, installs the local Cluster SSH key on the upstream master and pushes the registration operations back. The SSH password is used only for that request and is not saved. If this local install already has cluster operations for a different cluster, self-join refuses to overwrite them unless the recovery option is enabled. Recovery archives the previous local cluster state under `data/cluster/archives/` before replacing it with the upstream cluster state.

PBCluster SSH access is technical setup state. During normal PBGui setup/update on a VPS, PBGui now creates a dedicated local PBCluster SSH key and installs the master's public key on the VPS with a forced command that can only run `cluster_sync_command.py`. PBCluster uses this dedicated key with `IdentitiesOnly=yes`; users do not need to create or copy SSH keys manually.

VPS nodes do not initiate SSH fanout to other peers by default. A runner VPS only contacts explicit `sync_peers`; this avoids accidental VPS-to-VPS meshes. Masters can still push to reachable VPS nodes unless their outbound peer list is explicitly restricted.

Use **Edit** on a node to configure its allowed outbound peers. Use **Repair SSH** after changing peer allowlists or after updating a node: it reads the remote PBCluster public key, stores its fingerprint in cluster metadata, and installs the required restricted keys for the master and any configured peer sources.

When a joined node shows **OK**, the **Preview** action reads the remote state vector and desired state. It compares actor sequence numbers, V7 instance metadata, tombstones and API-key metadata against local state. It also calculates which local operations the remote is missing, which remote operation ranges are missing locally, and which hash references a later write phase would need. Preview is read-only; it does not copy operations, blobs or configs.

From the Preview window, **Push Missing Ops + Rebuild** is an explicit retry/diagnostic remote write action. It is available only when the remote has no operations missing locally. It starts one backend push job that sends current V7 config blobs, API-key payload blobs, API-key secret blobs, bulk-sends the local oplog entries the remote state-vector lacks, reports local progress while the job runs, and then runs remote `rebuild`. Progress reporting does not split or slow the remote sync. If the remote wrapper is older and cannot accept the bulk commands yet, PBGui falls back to slower per-item uploads where available.

After operations and config blobs are synchronized, the Preview window also shows **V7 Config Materialization Preview**. **Materialize V7 Configs** is the manual retry action for writing assigned, non-conflicted V7 JSON configs from verified config blobs into the remote `data/run_v7` directory. It refuses to run when the remote state-vector or desired state differs from local state, or when required blobs are missing or invalid. It skips configs assigned to other nodes, conflicted configs and tombstoned instances.

The Preview window also shows **API-key Materialization Preview**. **Materialize API Keys** is the manual retry action for installing `api-keys.json` from the replicated secret blob. Master nodes create a normal `data/api-keys/` backup first when an existing file differs; VPS runner nodes skip local backups. The write is atomic and verifies the final hash.

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

---

## Safety rules

- Do not delete local bot directories to signal a delete. Use PBGui so it writes a delete operation.
- Do not reuse copied `data/cluster/node_id` files on another installation.
- Do not edit `desired_state.json` manually; it is generated from the operation log.
- Keep admin SSH access available for recovery even though cluster replication uses restricted keys.
