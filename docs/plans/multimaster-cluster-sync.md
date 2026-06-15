# Multi-Master Cluster Sync Spec

## Scope

In scope:

- Multi-master and multi-VPS replicated cluster state.
- V7 desired state: config upserts, moves, starts, stops, deletes, tombstones.
- Explicit V7 forced-mode config changes such as Panic, Graceful Stop, and Take Profit Only.
- Syncable V7 JSON config files, including coin override JSON files.
- `api-keys.json` distribution and metadata.
- Restricted cluster-sync SSH keys.
- Dedicated Cluster UI page.

Out of scope:

- DB Tools row sync and database file copy.
- Dashboard/template sync.
- External state services.
- Master-to-master inbound connectivity requirement.
- Automatic panic decisions, automatic forced selling, or duplicate-bot liquidation behavior.

## State Directory

```text
data/cluster/
  cluster_id
  node_id
  node_identity.json
  cluster_nodes.json
  desired_state.json
  state_vector.json
  oplog/
    <actor_node_id>/
      00000001.json
  config_blobs/
    sha256/<first2>/<hash>.json
  secret_blobs/
    sha256/<first2>/<hash>.json
```

Rules:

- JSON files use `indent=4`.
- Local writes use temp file in the same directory plus `os.replace()`.
- Remote writes use the restricted cluster command wrapper and temp file plus rename.
- Direct overwrite of materialized state files is forbidden.
- `secret_blobs/` stores sensitive payloads such as `api-keys.json` and must use owner-only permissions.

## Identity

`cluster_id`:

- Format: `pbgui-cluster-<uuid4>`.
- Stored in `data/cluster/cluster_id`.
- Generated once on the first cluster master.
- Copied to joining nodes.
- Never changes automatically.
- Different remote `cluster_id` means foreign cluster; block sync.

`node_id`:

- Format: `pbgui-node-<uuid4>`.
- Stored in `data/cluster/node_id`.
- Generated once per PBGui installation.
- Never changes for hostname, IP, SSH port, VPS Manager name, or `pbname` changes.
- Used for oplog `actor`, `op_id`, `assigned_host`, and `state_vector.json` keys.
- `pbname`, hostname, IP, SSH user, SSH port, and host key are mutable metadata only.

`node_identity.json`:

```json
{
    "schema_version": 1,
    "cluster_id": "pbgui-cluster-...",
    "node_id": "pbgui-node-...",
    "created_at": 1780734910,
    "created_from_pbname": "magicnuc1",
    "role": "master"
}
```

Join rules:

- No remote cluster files: install local `cluster_id`, create/install remote `node_id`, write `ADD_NODE`.
- Same `cluster_id`: accept existing node or write `ADD_NODE`.
- Different `cluster_id`: block sync.
- Duplicate `node_id` on two reachable installations: block sync until resolved.
- Data directory survived reinstall: keep `node_id`.
- Data directory lost: create new `node_id`; old node must be disabled or replaced explicitly.

## Materialized Files

`cluster_nodes.json` is rebuilt from membership operations:

```json
{
    "schema_version": 1,
    "cluster_id": "pbgui-cluster-...",
    "generation": 42,
    "nodes": {
        "pbgui-node-...": {
            "node_id": "pbgui-node-...",
            "role": "master",
            "pbname": "magicnuc1",
            "display_name": "magicnuc1",
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

`desired_state.json` is rebuilt from instance and API-key operations:

```json
{
    "schema_version": 1,
    "cluster_id": "pbgui-cluster-...",
    "generated_at": 1780735200,
    "instances": {
        "bybit_BTC": {
            "version": "v19",
            "desired_state": "running",
            "assigned_host": "pbgui-node-...",
            "config_manifest_hash": "sha256:...",
            "updated_by": "pbgui-node-...",
            "updated_at": 1780735010,
            "conflicted": false
        }
    },
    "tombstones": {},
    "api_keys": {
        "serial": 42,
        "payload_hash": "sha256:...",
        "updated_by": "pbgui-node-...",
        "updated_at": 1780735010
    }
}
```

`state_vector.json` maps actor node IDs to highest known sequence:

```json
{
    "pbgui-node-...": 124
}
```

## Operation Log

Path:

```text
data/cluster/oplog/<actor_node_id>/<seq:08d>.json
```

Envelope required on every operation:

```json
{
    "schema_version": 1,
    "cluster_id": "pbgui-cluster-...",
    "op_id": "pbgui-node-...:00000124",
    "actor": "pbgui-node-...",
    "seq": 124,
    "op": "MOVE_INSTANCE",
    "created_at": 1780735010
}
```

Rules:

- `op_id` is `<actor>:<seq:08d>`.
- `seq` is `1 + max(seq)` for the local actor, allocated under a local write lock.
- Operation file path must match `actor` and `seq`.
- Apply is idempotent.
- Rebuild order is `(actor, seq, op_id)`.
- Reject unknown `schema_version`, foreign `cluster_id`, invalid names, path traversal, and malformed payloads.

Membership operations:

- `ADD_NODE`: create or update node metadata.
- `UPDATE_NODE`: update mutable display/role/flags metadata.
- `UPDATE_NODE_ADDRESS`: update SSH host/port/address fields.
- `UPDATE_NODE_SSH`: update SSH user, port, remote path, or host key fields.
- `UPDATE_NODE_KEY`: update cluster-sync public key metadata.
- `DISABLE_NODE`: set `enabled=false` and stop scheduling sync to the node.

V7 operations:

- `UPSERT_CONFIG`: set instance version, desired state, assigned host, config manifest hash, and explicit forced-mode changes saved through Dashboard or Run Config UI.
- `MOVE_INSTANCE`: change assigned host and version.
- `START_INSTANCE`: set `desired_state=running`.
- `STOP_INSTANCE`: set `desired_state=stopped`.
- `DELETE_INSTANCE`: remove active instance and create tombstone.
- `TOMBSTONE_INSTANCE`: prevent deleted config resurrection.

API-key operation:

- `UPSERT_API_KEYS`: set API-key serial, payload hash, and secret blob hash.

## V7 Config Manifest

Manifest input:

- Include every syncable JSON file in `data/run_v7/<instance>/`.
- Include `config.json` and coin override JSON files.
- Exclude `config_run.json`, `running_version.txt`, `ignored_coins.json`, `approved_coins.json`, logs, and non-JSON runtime files.

Manifest format before hashing:

```json
{
    "schema_version": 1,
    "files": {
        "BTC.json": {"sha256": "...", "size": 1234},
        "config.json": {"sha256": "...", "size": 5678}
    }
}
```

Hash rule:

- Sort filenames lexicographically.
- Serialize manifest with stable JSON separators.
- `config_manifest_hash = "sha256:" + sha256(serialized_manifest)`.
- Blob paths are content-addressed by each file hash under `config_blobs/`.

## API Keys

`UPSERT_API_KEYS` operation payload:

```json
{
    "api_serial": 42,
    "payload_hash": "sha256:...",
    "secret_blob_hash": "sha256:..."
}
```

Rules:

- Do not log API-key content.
- Do not place API-key content in `desired_state.json`.
- Store payload bytes only in `secret_blobs/` with owner-only permissions.
- Install through the same backup, verify, and bot restart rules as current API Sync.

## Conflict Rules

Conflict condition:

- Two or more operations modify the same instance from the same `parent_version` and produce different versions, hosts, desired states, or manifest hashes.

Conflict result:

- Set `conflicted=true` in `desired_state.json`.
- Store competing `op_id`, parent version, proposed version, assigned host, desired state, and manifest hash.
- PBRun must not auto-start conflicted instances.
- Resolution writes a new operation with a new version.

No blind last-write-wins for instance conflicts.

## Delete And Move Rules

Delete:

- Never infer delete from missing files, missing remote instances, or missing `status_v7.json` entries.
- Only `DELETE_INSTANCE` and `TOMBSTONE_INSTANCE` delete or archive local files.
- Tombstones prevent old configs from being reintroduced by stale peers.

Move:

- Move is explicit and versioned.
- A moved instance must not start on the old host after the old host learns the move operation.

## PBRun Gate

Before starting a V7 bot, PBRun must verify:

- `desired_state.json` exists and is readable.
- Instance exists in desired state.
- Instance is not tombstoned.
- Instance is not conflicted.
- `desired_state == "running"`.
- `assigned_host == local node_id`.
- Local config manifest hash matches `config_manifest_hash`.
- Local config version matches `version`.

If any check fails:

- Do not start the bot.
- Log the blocked reason.
- Surface blocked state in PBGui.

Stale-state policy:

- Stale local desired state is warning-only.
- PBRun must not block startup only because cluster state is old.
- If no peer can refresh state, log and surface a stale-state warning.

## Sync Protocol

Normal replication uses restricted cluster-sync SSH keys and forced commands, not unrestricted shell or SFTP.

Bootstrap may use existing VPS Manager SSH credentials only for import, key installation, and recovery.

Regular replication is owned by a lightweight `PBCluster` daemon that runs on every cluster node, including runner VPS hosts. Runner VPS hosts must not require `PBApiServer.py` for Cluster Sync. `PBApiServer.py` remains a master/UI/API service only.

Required verbs:

- `hello`: return node identity and protocol version.
- `get-state-vector`: return `state_vector.json`.
- `get-ops`: return a bounded range of operation-log entries for one actor.
- `get-blob` / `get-blobs`: return verified config blobs by hash.
- `get-secret-blob`: return a verified secret blob by hash to an authenticated cluster peer.
- `put-op`: validate and atomically write one operation.
- `put-blob`: validate hash and atomically write one config blob.
- `put-secret-blob`: validate hash and atomically write one secret blob with restricted permissions.
- `rebuild`: rebuild materialized files from oplog.
- `get-desired-state`: return `desired_state.json`.
- `install-api-keys`: install validated API-key payload with backup, verify, and restart rules.

Implementation note: the restricted wrapper implements identity, state-vector, operation, blob, secret-blob, rebuild, desired-state, and materialization verbs. `install-api-keys` remains a later safety subphase because it must reuse the existing API-key backup, validation, and bot-restart rules.

Peer reconcile loop:

1. Node A calls Node B `hello`.
2. Node A verifies matching `cluster_id`.
3. Node A reads Node B `state_vector.json`.
4. Node A pulls operation ranges and required blobs that it is missing.
5. Node A sends operations and required blobs that Node B is missing.
6. Any node that received new operations rebuilds materialized state.
7. Any node that received new operations or blobs materializes local V7 configs and API keys assigned to itself.
8. If Node A changed Node B, Node A requests remote rebuild and remote local materialization. Materialization never starts or stops bots directly.

Propagation model:

- Push-on-change is the fast path. When `PBCluster` sees a new local operation, it schedules immediate fanout to all reachable `sync_enabled` peers.
- Received-operation fanout is also required. When a node receives a new operation from a peer, it rebuilds locally, materializes locally, and schedules fanout to other peers so updates can traverse the cluster even when the original writer cannot reach every node.
- Periodic reconcile is the repair path. Each node periodically compares state vectors with peers and repairs missed operations, missing blobs, interrupted transfers, stale materialization and nodes that were offline during a change.
- Boot reconcile is required for runner VPS hosts. On startup, `PBCluster` attempts a short peer sync window before PBRun relies on local desired state. If no peer is reachable, PBRun may continue with local desired state and a stale-state warning.
- All transfers are idempotent. Operation identity is `(actor, seq, op_id)`, duplicate writes with identical content are accepted, and state-vector equality means no payload transfer is needed.

Service responsibilities:

- `PBCluster.py`: lightweight daemon on masters and runner VPS hosts. It performs boot sync, periodic sync, push-on-change fanout, received-operation fanout, local rebuild, local materialization, sync status writes and retry backoff.
- `PBRun.py`: local bot supervisor only. It gates start/continue decisions against local desired state and surfaces blocked/stale reasons. It does not contact peers or write remote cluster files.
- `PBApiServer.py`: master-only UI/API service. It writes local cluster operations through normal PBGui actions and exposes Cluster UI/status/repair endpoints. It is not required on runner VPS hosts.
- `cluster_sync_command.py`: restricted SSH command wrapper used by `PBCluster` and master repair actions for remote peer RPC.

## Restricted SSH Key Model

Each node has a dedicated cluster-sync key.

Install public keys in `authorized_keys` with restrictions:

```text
restrict,no-pty,no-agent-forwarding,no-X11-forwarding,no-port-forwarding,no-user-rc,command="/home/mani/software/pbgui/cluster_sync_command.py --remote-node <peer_node_id>" ssh-ed25519 AAAA... pbgui-cluster:<peer_node_id>
```

Optional when stable:

- Add `from="ip1,ip2"`.

Wrapper requirements:

- Read `SSH_ORIGINAL_COMMAND`.
- Reject unknown verbs.
- Enforce matching `cluster_id`.
- Enforce known or explicitly joining peer `node_id`.
- Reject path traversal and caller-provided absolute paths.
- Allow writes only under `data/cluster/`, except `install-api-keys`.
- Enforce size limits.
- Verify hashes.
- Use atomic writes.
- Do not allow shell execution, command chaining, unrestricted SFTP, port forwarding, pty, or agent forwarding.

## Firewall Rules

VPS-to-VPS SSH firewall rules are managed automatically.

Rules:

- Derive peer allow rules from enabled nodes in `cluster_nodes.json` with `state_replica=true` and `sync_enabled=true`.
- Allow peer VPS IPs to reach the configured SSH port used for restricted cluster-sync commands.
- Apply new allow rules before enabling peer sync to that node.
- Remove obsolete allow rules only after replacement connectivity has been confirmed.
- Do not remove non-PBGui SSH rules.
- Log every firewall change and show failures on the Cluster page.

## `status_v7.json` Compatibility

- First cluster-sync release: keep generating and consuming `status_v7.json` for compatibility.
- During compatibility, `desired_state.json` is authority and `status_v7.json` is generated output.
- Next version after cluster-sync: remove old `status_v7.json` authority/reconcile behavior.

## Cluster UI

Dedicated page: `Cluster`.

The first local page shows local cluster identity, materialized nodes, V7 desired state, tombstones, recent oplog entries, and a local bootstrap preview/apply action. Bootstrap writes explicit `ADD_NODE` operations for known VPS Manager hosts and `UPSERT_CONFIG` operations for local V7 configs; it uses VPS Monitor role metadata when available, does not infer deletes from missing files or missing VPS entries, and does not clear tombstones. Later versions must add:

- Remote node reachability, sync enabled state, and last seen. Initial read-only `hello` probe status is implemented before write sync.
- Desired-state generation time and stale warnings.
- V7 conflicts and competing operations.
- API-key sync status per node.
- Foreign cluster and duplicate node-id blockers.

## First Runnable Version Requirements

The first runnable Cluster Sync version requires items 1-7. They may be implemented incrementally on the branch, but they are not optional post-release phases:

1. Cluster state skeleton: identity files, operation writer, rebuild logic, membership ops, tests.
2. V7 operation log: V7 ops, manifest hash, desired state, tombstones, no delete inference.
3. API-key operation log: `UPSERT_API_KEYS`, secret blobs, install path compatibility.
4. Restricted SSH transport: key generation, authorized-key install, forced command wrapper, master-to-VPS and VPS-to-VPS RPC.
5. PBRun gate: desired-state checks and stale warning-only policy.
6. `PBCluster` daemon on masters and runner VPS hosts: push-on-change fanout, received-operation fanout, boot sync, periodic reconcile, local rebuild and local materialization.
7. Cluster UI: node status, stale warnings, conflicts, API-key status and repair/manual-sync actions.

After the first runnable Cluster Sync version:

8. Remove old `status_v7.json` authority in the next version.

## Tests

Unit tests:

- Identity creation and migration from `pbname`.
- Foreign cluster rejection.
- Duplicate `node_id` rejection.
- Operation envelope validation.
- Oplog idempotent apply and deterministic rebuild.
- Membership materializes `cluster_nodes.json`.
- Bootstrap registers known VPS Manager hosts as nodes without remote mutation.
- `UPSERT_CONFIG`, `MOVE_INSTANCE`, `START_INSTANCE`, `STOP_INSTANCE`, `DELETE_INSTANCE`, `TOMBSTONE_INSTANCE` materialize correctly.
- Tombstone prevents stale config resurrection.
- Conflict detection for same parent version.
- `config_manifest_hash` changes when any syncable V7 JSON changes.
- `UPSERT_API_KEYS` updates metadata without exposing secrets in logs or desired state.
- Restricted command rejects unknown verbs, path traversal, foreign cluster, and malformed payloads.
- PBRun gate blocks tombstoned, conflicted, wrong-host, wrong-version, wrong-hash instances.
- PBRun warns but does not block on stale local state.

Integration tests:

- Master imports VPS and installs cluster identity.
- Master distributes cluster state to VPS through restricted command.
- VPS reboot starts only locally assigned bots.
- Bot move from offline VPS to another VPS.
- Old VPS returns, peer sync learns move, old bot stays stopped.
- Master offline, VPS reboot uses local desired state.
- No peer reachable plus stale local desired state produces warning-only boot.
- Automatic firewall rule creation for VPS-to-VPS peer sync.
- Obsolete PBGui-managed firewall rules are removed only after replacement connectivity is confirmed.
