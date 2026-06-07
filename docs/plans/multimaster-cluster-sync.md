# Multi-Master Cluster Sync Plan

## Goal

PBGui should support robust multi-master and multi-VPS synchronization without external services such as rclone, Synology, Upstash, or Supabase.

The system should:

- support multiple masters
- support multiple VPS runners
- support masters behind firewalls with no inbound access
- support VPS nodes without DNS names
- track IP addresses and SSH access explicitly
- synchronize config updates, moves, starts, stops, and deletes as explicit operations
- avoid deriving deletes from missing files
- prevent stale VPS nodes from starting moved/deleted bots when they can learn newer cluster state from peers
- allow normal VPS reboot operation from local cluster state when no master is online
- replace `status_v7.json` as the long-term source of truth

## Non-Goals

- No large implementation in the current `test-installer` branch.
- No external SaaS state backend.
- No master-to-master inbound connectivity requirement.
- No passivbot log based duplicate detection as a primary safety mechanism.
- No panic or forced-sell behavior for suspected duplicate bots.

## Core Idea

Every master and every VPS stores a local replicated cluster state.

Cluster state lives under:

```text
data/cluster/
  cluster_id
  node_id
  cluster_nodes.json
  desired_state.json
  state_vector.json
  oplog/
  config_blobs/
```

Sync is done over SSH/SFTP between nodes that can reach each other.

Masters do not need inbound connections. Masters push/pull cluster state outbound to VPS nodes. VPS nodes may also sync with other VPS peers if firewall and SSH keys allow it.

## File Layout

```text
data/cluster/
  cluster_id
  node_id
  cluster_nodes.json
  desired_state.json
  state_vector.json
  oplog/
    magicnuc1/
      00000001.json
      00000002.json
    magicnuc2/
      00000001.json
  config_blobs/
    sha256/
      ab/
        abcdef....json
```

## Node Identity

Each node gets a stable `node_id`.

Examples:

```text
magicnuc1
magicnuc2
manibot93
manibot94
```

The `node_id` is not DNS. IP address, SSH user, SSH port, and host key fingerprint are tracked separately.

## Cluster Membership

`cluster_nodes.json` is a materialized snapshot built from membership operations in the oplog.

Example:

```json
{
  "cluster_id": "pbgui-main",
  "generation": 42,
  "nodes": {
    "magicnuc1": {
      "node_id": "magicnuc1",
      "role": "master",
      "reachable_by_peers": false,
      "actor_enabled": true,
      "sync_enabled": true,
      "enabled": true
    },
    "manibot93": {
      "node_id": "manibot93",
      "role": "vps",
      "ssh_host": "167.160.188.196",
      "ssh_port": 22,
      "ssh_user": "mani",
      "ssh_host_key_sha256": "SHA256:...",
      "remote_pbgui_dir": "/home/mani/software/pbgui",
      "cluster_dir": "/home/mani/software/pbgui/data/cluster",
      "bot_runner": true,
      "state_replica": true,
      "sync_enabled": true,
      "enabled": true,
      "last_seen": 1780734910
    }
  }
}
```

## Membership Operations

Membership changes are written as operations, not direct JSON edits.

Examples:

```json
{"op": "ADD_NODE", "node_id": "manibot93", "role": "vps", "ssh_host": "167.160.188.196"}
```

```json
{"op": "UPDATE_NODE_ADDRESS", "node_id": "manibot93", "ssh_host": "167.160.188.200"}
```

```json
{"op": "UPDATE_NODE_KEY", "node_id": "manibot93", "public_key": "ssh-ed25519 ..."}
```

```json
{"op": "DISABLE_NODE", "node_id": "old-vps"}
```

## Operation Log

Every instance change is an append-only operation.

Examples:

```json
{
  "op_id": "magicnuc1:00000123",
  "actor": "magicnuc1",
  "seq": 123,
  "op": "UPSERT_CONFIG",
  "instance": "bybit_BTC",
  "parent_version": "v17",
  "version": "v18",
  "config_hash": "sha256:...",
  "assigned_host": "manibot93",
  "desired_state": "running",
  "created_at": 1780734910
}
```

```json
{
  "op_id": "magicnuc1:00000124",
  "actor": "magicnuc1",
  "seq": 124,
  "op": "MOVE_INSTANCE",
  "instance": "bybit_BTC",
  "parent_version": "v18",
  "version": "v19",
  "from": "manibot93",
  "to": "manibot94",
  "created_at": 1780735010
}
```

```json
{
  "op_id": "magicnuc1:00000125",
  "actor": "magicnuc1",
  "seq": 125,
  "op": "DELETE_INSTANCE",
  "instance": "bybit_BTC",
  "parent_version": "v19",
  "version": "v20",
  "created_at": 1780735110
}
```

Required operation types:

- `ADD_NODE`
- `UPDATE_NODE`
- `UPDATE_NODE_ADDRESS`
- `UPDATE_NODE_KEY`
- `DISABLE_NODE`
- `UPSERT_CONFIG`
- `MOVE_INSTANCE`
- `START_INSTANCE`
- `STOP_INSTANCE`
- `DELETE_INSTANCE`
- `TOMBSTONE_INSTANCE`

## Desired State

`desired_state.json` is built deterministically from the oplog.

Example:

```json
{
  "cluster_id": "pbgui-main",
  "generated_at": 1780735200,
  "instances": {
    "bybit_BTC": {
      "version": "v19",
      "desired_state": "running",
      "assigned_host": "manibot94",
      "config_hash": "sha256:...",
      "updated_by": "magicnuc1",
      "updated_at": 1780735010,
      "conflicted": false
    }
  },
  "tombstones": {
    "old_bot": {
      "version": "v20",
      "deleted_by": "magicnuc1",
      "deleted_at": 1780735110
    }
  }
}
```

## Conflict Handling

If two masters change the same instance from the same `parent_version`, the result is a conflict.

Rules:

- Do not use blind last-write-wins.
- Mark the instance as `conflicted=true`.
- PBRun must not auto-start conflicted instances.
- UI must show conflict details and offer explicit resolution.
- Conflict resolution creates a new operation with a new version.

## Delete Rule

Delete must never be inferred from missing remote files or missing remote instances.

Only explicit operations may delete:

- `DELETE_INSTANCE`
- `TOMBSTONE_INSTANCE`

Local files may only be deleted or archived after a valid delete/tombstone operation exists in desired state.

## Move Rule

Move is explicit and versioned.

A move updates:

- `assigned_host`
- `version`
- optionally `desired_state`

The old VPS must not start the bot after it learns the move operation.

## PBRun Start Gate

PBRun checks cluster state before each bot start.

Required conditions:

- `desired_state.json` exists and is readable.
- Instance exists in desired state.
- Instance is not tombstoned.
- Instance is not conflicted.
- `desired_state == "running"`.
- `assigned_host == local node_id`.
- Local config hash matches `config_hash`.
- Local config version matches `version`.

If any condition fails:

- Do not start the bot.
- Write a clear log entry.
- Surface the blocked-start state in PBGui.

## Normal VPS Reboot

A VPS should keep working without an online master.

Boot flow:

1. VPS starts.
2. Cluster sync tries to contact known peer VPS nodes for a short time.
3. If peers are reachable, pull missing operations and rebuild desired state.
4. If no peers are reachable, use local desired state.
5. PBRun starts only bots assigned to this VPS in local desired state.

Open policy decision:

- Whether to block starts if desired state is older than a configured age.
- Proposed default: allow normal reboot from local desired state, but show warnings for stale state.

## Critical Move Scenario

Scenario:

- `manibot93` is down.
- Bot is moved to `manibot94`.
- All masters go down.
- `manibot93` starts again.

Expected behavior:

- Move operation was pushed to at least `manibot94`.
- `manibot93` boots and tries peer sync.
- If `manibot94` is reachable, `manibot93` learns the move.
- Desired state says `assigned_host=manibot94`.
- PBRun on `manibot93` does not start the old bot.

Extreme case:

- `manibot93` cannot reach any peer.
- No system can know about changes made during downtime.
- Policy must decide between availability and safety.

Recommendation:

- Use local desired state for normal reboot.
- Warn when state is stale.
- Consider blocking starts when state is very stale and no peers are reachable.

## Sync Topology

Not every node must reach every other node.

Example:

```text
magicnuc1 -> manibot93
magicnuc2 -> manibot94
manibot93 <-> manibot94
manibot94 <-> manibot95
```

Operations propagate through reachable paths.

## SSH/SFTP Sync Protocol

Minimal protocol:

1. Node A reads Node B `state_vector.json`.
2. Node A computes operations missing on B.
3. Node A copies missing operations to B.
4. Node A copies missing config blobs to B.
5. Node B rebuilds `desired_state.json`.
6. Optionally repeat in the opposite direction.

`state_vector.json` example:

```json
{
  "magicnuc1": 124,
  "magicnuc2": 55,
  "manibot93": 8
}
```

## SSH Keys

Password should be used only for bootstrap/import.

Long-term approach:

- Each node has a cluster-sync SSH key.
- Public keys are distributed as `UPDATE_NODE_KEY` operations.
- Master installs keys on reachable VPS nodes.
- VPS nodes maintain authorized keys for known sync peers.
- Keys should be tagged with comments like `pbgui-cluster:<node_id>`.

## Firewall

For VPS-to-VPS sync:

- `cluster_nodes.json` includes each VPS IP and SSH port.
- PBGui can derive UFW rules from membership.
- Master IPs may access SSH.
- Peer VPS IPs may access SSH when `state_replica=true`.
- Old IP rules are removed only after replacement connectivity is confirmed.

## Role Of status_v7.json

`status_v7.json` remains a compatibility layer during migration.

Long term:

- `desired_state.json` becomes the authority.
- `status_v7.json` may be generated from desired state while old code still needs it.
- UI and PBRun should eventually read desired state directly.

## Migration Phases

### Phase 1: Cluster State Skeleton

- Add `data/cluster` structure.
- Generate `cluster_nodes.json` from VPS Manager hosts.
- Existing VPS import writes `ADD_NODE`.
- VPS setup writes `ADD_NODE`.
- No PBRun behavior change yet.

### Phase 2: V7 Operation Log

- Write operations for V7 config save, activate, move, stop, start, and delete.
- Build `desired_state.json` locally from oplog.
- Stop deriving deletes from missing remote data.
- Keep writing `status_v7.json` for compatibility.

### Phase 3: Master-to-VPS State Distribution

- Master pushes cluster state to all reachable VPS nodes via SSH/SFTP.
- VPS stores `data/cluster` locally.
- UI shows cluster sync status per node.

### Phase 4: PBRun Start Gate

- PBRun checks desired state before starting bots.
- Block starts for tombstoned, conflicted, wrong-host, wrong-version, or wrong-hash instances.
- Surface blocked starts in logs/UI.

### Phase 5: VPS-to-VPS Peer Sync

- VPS nodes sync cluster ops with peer VPS nodes on boot and periodically.
- Manage cluster SSH keys.
- Optionally manage firewall rules for peer SSH.

### Phase 6: Conflict UI

- Show conflicted instances.
- Show competing operations and config hashes.
- Let user choose a winning version or create a merged config.
- Resolution writes a new operation.

## Tests

Unit tests:

- Oplog apply/rebuild desired state.
- Membership operations materialize `cluster_nodes.json`.
- `UPSERT_CONFIG`, `MOVE_INSTANCE`, `DELETE_INSTANCE` update desired state correctly.
- Tombstone prevents old config resurrection.
- Conflict detection for two ops with same parent version.
- PBRun start gate allows only correct `assigned_host`.
- PBRun blocks tombstoned/conflicted/wrong-host instances.

Integration tests:

- Master imports VPS and distributes cluster state.
- VPS reboots and starts only assigned bots.
- Bot moves from offline VPS to another VPS.
- Old VPS returns and learns the move from a peer.
- Master offline, VPS reboot uses local desired state.
- No peer reachable and local desired state stale.

## Open Decisions

- How stale may local desired state be before PBRun blocks starts?
- Should stale state be warning-only or blocking?
- Should VPS-to-VPS SSH firewall rules be fully automatic?
- Should cluster sync use a dedicated SSH key or the existing user key?
- How long should `status_v7.json` compatibility be maintained?
- Where should the UI show cluster conflicts and node sync status?

## Recommendation

Do not build this as a big bang.

Start with:

- cluster state file layout
- membership operations
- V7 operation log
- desired state builder
- explicit tombstones and moves

Then add:

- master-to-VPS distribution
- PBRun start gate
- VPS-to-VPS peer sync
- conflict UI

This keeps the current branch focused on v1.83/installer stability while providing a clear path to robust multi-master sync later.
